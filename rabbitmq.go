package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type ExchangeName string
type QueueName string

// 队列
type Queue struct {
	RoutingKey string // 路由键
}

// 交换机
type Exchange struct {
	ExchangeType string // 交换机类型
	// 交换机绑定的队列列表，如果有延迟时间，则生成 队列名_(秒)s_transfer，
	// 如果修改延迟时间，则生成新的队列并绑定，然后解绑旧的队列，旧队列需要确认消费完毕后手动删除
	BindQueues map[QueueName]*Queue
}

var (
	once                                 sync.Once
	mq                                   *rabbitMQ
	waitDeleteBindExchangeDelayQueue     = make(map[QueueName]struct{}) // 绑定过交换机的延迟队列
	waitDeleteBindExchangeDelayQueueLock sync.Mutex
)

type rabbitMQ struct {
	conn                 *amqp.Connection
	notifyClose          chan *amqp.Error
	ackRetryTime         int                                      // 单个消息确认重试次数
	queueDelayMap        map[QueueName]map[time.Duration]struct{} // 没有绑定到交换机的迟时队列延
	exchangeMap          map[ExchangeName]*Exchange               // 交换机队列定义
	queueExchangeMap     map[QueueName]ExchangeName               // 队列绑定到交换机
	consumesRegisterLock sync.Mutex
	consumes             map[string]struct{} // 消费者去重
	host                 string
	port                 int
	managerPort          int
	username             string
	password             string
	vhost                string
}

func GetRabbitMQ() *rabbitMQ {
	once.Do(func() {
		mq = &rabbitMQ{
			notifyClose:      make(chan *amqp.Error),
			ackRetryTime:     3,
			queueDelayMap:    make(map[QueueName]map[time.Duration]struct{}),
			exchangeMap:      make(map[ExchangeName]*Exchange),
			queueExchangeMap: make(map[QueueName]ExchangeName),
			consumes:         make(map[string]struct{}),
			managerPort:      15672,
		}
	})
	return mq
}

func (r *rabbitMQ) SetManagerPort(port int) {
	r.managerPort = port
}

func (r *rabbitMQ) Conn(host string, port int, user, password, vhost string) (err error) {
	r.host = host
	r.port = port
	r.username = user
	r.password = password
	if vhost == "/" {
		return errors.New("请不要使用/作为vhost")
	}
	r.vhost = vhost
	return r.reConn()
}

func (r *rabbitMQ) reConn() (err error) {
	if r.conn == nil || r.conn.IsClosed() {
		url := fmt.Sprintf("amqp://%s:%s@%s:%d%s", r.username, r.password, r.host, r.port, r.vhost)
		r.conn, err = amqp.Dial(url)
		if err != nil {
			return errors.Wrap(err, "连接RabbitMQ失败")
		}
		r.conn.NotifyClose(r.notifyClose)
	}
	return
}

func (r *rabbitMQ) Close() error {
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

// 定义交换机和队列
func (r *rabbitMQ) ExchangeQueueCreate(declare map[ExchangeName]*Exchange) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return errors.Wrap(err, "连接RabbitMQ失败")
	}
	defer func(ch *amqp.Channel) {
		_ = ch.Close()
	}(ch)

	r.exchangeMap = declare
	for exchangeName, exchange := range r.exchangeMap {
		if exchange.ExchangeType == "" {
			// 交换机类型默认为直连
			exchange.ExchangeType = amqp.ExchangeDirect
		}
		err = ch.ExchangeDeclare(string(exchangeName), exchange.ExchangeType, true, false, false, false, nil)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("定义交换机%s错误", exchangeName))
		}
	}
	for exchangeName, exchange := range r.exchangeMap {
		for queueName, v := range exchange.BindQueues {
			// 定义队列
			_, err = ch.QueueDeclare(string(queueName), true, false, false, false, nil)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("定义队列%s错误", queueName))
			}

			switch exchange.ExchangeType {
			case amqp.ExchangeDirect:
			// 绑定直连类型有没有路由都可以
			case amqp.ExchangeFanout:
				// 绑定扇出类型不需要路由
				v.RoutingKey = ""
			default:
				return errors.New(fmt.Sprintf("未定义的交换机类型%s", exchange.ExchangeType))
			}

			// 绑定路由
			err = ch.QueueBind(string(queueName), v.RoutingKey, string(exchangeName), false, nil)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("队列%s绑定交换机%s失败", queueName, exchangeName))
			}

			// 定义队列对应的死信接收队列
			dlxName := r.generateDlxQueueName(queueName)
			_, err = ch.QueueDeclare(string(dlxName), true, false, false, false, nil)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("定义死信队列%s错误", dlxName))
			}
			r.queueExchangeMap[queueName] = exchangeName
		}
	}
	return nil
}

// 定义延迟队列并绑定到交换机（只能动态读取数据库配置绑定，不要配置写死），延迟队列名格式为 新交换机名_(秒)s_transfer
// 用途，数据库配置订单创建15分钟自动取消，后面又改成30分钟，则需要重新定义延迟队列，绑定到交换机，然后解绑旧队列，旧队列没有数据则删除
// 同名不同时间的队列会解绑，并定时检测是否有数据，没数据会删除
func (r *rabbitMQ) BindDelayQueueToExchange(fromExchangeName, toExchangeName ExchangeName, delay time.Duration) error {
	exchangeDelayQueueName := r.getBindExchangeDelayQueueName(toExchangeName, delay)
	err := r.declareBindExchangeDelayQueue(toExchangeName, exchangeDelayQueueName, delay)
	if err != nil {
		return err
	}

	ch, err := r.conn.Channel()
	if err != nil {
		return errors.Wrap(err, "获取通道失败")
	}
	defer func(ch *amqp.Channel) {
		_ = ch.Close()
	}(ch)
	// 绑定路由
	err = ch.QueueBind(string(exchangeDelayQueueName), "", string(fromExchangeName), false, nil)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("队列%s绑定交换机%s失败", exchangeDelayQueueName, fromExchangeName))
	}

	var bindings []*queue
	bindings, err = r.getNeedUnbindDelayQueue(fromExchangeName)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("获取交换机%s需要解绑的延迟队列失败", fromExchangeName))
	}
	for _, binding := range bindings {
		if binding.Destination == exchangeDelayQueueName {
			continue
		}
		index := strings.Index(string(binding.Destination), "_transfer")
		if index != -1 {
			err = ch.QueueUnbind(string(binding.Destination), "", string(binding.Source), nil)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("解绑交换机%s的队列%s失败", binding.Source, binding.Destination))
			}
			waitDeleteBindExchangeDelayQueueLock.Lock()
			waitDeleteBindExchangeDelayQueue[binding.Destination] = struct{}{}
			waitDeleteBindExchangeDelayQueueLock.Unlock()
		}
	}

	return nil
}

// 生成死信队列名
func (r *rabbitMQ) generateDlxQueueName(qn QueueName) QueueName {
	return QueueName(fmt.Sprintf("%s_dlx", qn))
}

// 获取绑定到交换机的延迟队列名
func (r *rabbitMQ) getBindExchangeDelayQueueName(transferToExchangeName ExchangeName, delay time.Duration) QueueName {
	second := int64(delay / time.Second)
	return QueueName(fmt.Sprintf("%s_%ds_transfer", transferToExchangeName, second))
}

// 定义绑定到交换机的延迟队列
func (r *rabbitMQ) declareBindExchangeDelayQueue(exchangeName ExchangeName, queueName QueueName, delay time.Duration) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return errors.Wrap(err, "获取通道失败")
	}
	defer func(ch *amqp.Channel) {
		_ = ch.Close()
	}(ch)

	ttl := int64(delay / time.Millisecond)

	_, err = ch.QueueDeclare(string(queueName), true, false, false, false, amqp.Table{
		"x-message-ttl":             ttl, // 消息过期时间，毫秒
		"x-dead-letter-exchange":    string(exchangeName),
		"x-dead-letter-routing-key": "",
	})
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("定义延迟队列%s错误", queueName))
	}
	return nil
}

// 定义延迟队列
func (r *rabbitMQ) declareDelayQueue(queueName QueueName, delay time.Duration) (err error) {
	ch, err := r.conn.Channel()
	if err != nil {
		return errors.Wrap(err, "获取通道失败")
	}
	defer func(ch *amqp.Channel) {
		_ = ch.Close()
	}(ch)

	// 计算延迟时间
	ttl := int64(delay / time.Millisecond)

	delayQueueName := r.getDelayQueueName(queueName, delay)

	_, err = ch.QueueDeclare(string(delayQueueName), true, false, false, false, amqp.Table{
		"x-message-ttl":             ttl, // 消息过期时间，毫秒
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": string(queueName),
	})
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("定义延迟队列%s错误", delayQueueName))
	}
	return
}

// 获取延迟队列名
func (r *rabbitMQ) getDelayQueueName(queue QueueName, delay time.Duration) QueueName {
	second := int64(delay / time.Second)
	return QueueName(fmt.Sprintf("%s_%ds", queue, second))
}

func (r *rabbitMQ) getNeedUnbindDelayQueue(exchangeName ExchangeName) (bindings []*queue, err error) {
	// 创建基本认证
	url := fmt.Sprintf("http://%s:%d/api/exchanges%s/%s/bindings/source", r.host, r.managerPort, r.vhost, exchangeName)
	req, err := r.buildRequest(url)
	if err != nil {
		return nil, err
	}
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "请求获取交换机绑定队列列表接口失败")
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)
	// 读取响应体
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "读取交换机绑定队列列表接口响应体失败")
	}
	// 解析 JSON 响应
	bindings = make([]*queue, 0)
	err = json.Unmarshal(body, &bindings)
	if err != nil {
		return nil, errors.Wrap(err, "解析交换机绑定队列列表接口响应体失败")
	}
	return bindings, nil
}

func (r *rabbitMQ) buildRequest(url string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.Wrap(err, "构建接口请求失败")
	}
	req.SetBasicAuth(r.username, r.password)
	return req, nil
}

type queue struct {
	Source          ExchangeName `json:"source"`
	Vhost           string       `json:"vhost"`
	Destination     QueueName    `json:"destination"`
	DestinationType string       `json:"destination_type"`
	RoutingKey      string       `json:"routing_key"`
	Arguments       struct {
	} `json:"arguments"`
	PropertiesKey string `json:"properties_key"`
}
