package rabbitmq

import (
	"fmt"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
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
	Queues map[QueueName]*Queue
}

var (
	once sync.Once
	mq   *rabbitMQ
)

type rabbitMQ struct {
	conn                 *amqp.Connection
	sendRetryTime        int                                      // 单个消息发送重试次数
	queueDelayMap        map[QueueName]map[time.Duration]struct{} // 没有绑定到交换机的迟时队列延
	exchangeMap          map[ExchangeName]*Exchange               // 交换机队列定义
	queueExchangeMap     map[QueueName]ExchangeName               // 队列绑定到交换机
	consumesRegisterLock sync.Mutex
	consumes             map[string]*Consumer // 消费者
}

func GetRabbitMQ() *rabbitMQ {
	once.Do(func() {
		mq = &rabbitMQ{
			sendRetryTime:    3,
			queueExchangeMap: make(map[QueueName]ExchangeName),
			queueDelayMap:    make(map[QueueName]map[time.Duration]struct{}),
			consumes:         make(map[string]*Consumer),
		}
	})
	return mq
}

func (r *rabbitMQ) Conn(host string, port int, user, password, vhost string) (err error) {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d%s", user, password, host, port, vhost)
	r.conn, err = amqp.Dial(url)
	return
}

func (r *rabbitMQ) Close() error {
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

func (r *rabbitMQ) ExchangeQueueCreate(declare map[ExchangeName]*Exchange) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return errors.Wrap(err, "连接RabbitMQ失败")
	}
	defer func(ch *amqp.Channel) {
		if ch != nil {
			_ = ch.Close()
		}
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

		if len(exchange.Queues) == 0 {
			return errors.New(fmt.Sprintf("交换机%s定义错误，队列不能为空", exchangeName))
		}
	}
	for exchangeName, exchange := range r.exchangeMap {
		for queueName, queue := range exchange.Queues {
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
				queue.RoutingKey = ""
			default:
				return errors.New(fmt.Sprintf("未定义的交换机类型%s", exchange.ExchangeType))
			}

			// 绑定路由
			err = ch.QueueBind(string(queueName), queue.RoutingKey, string(exchangeName), false, nil)
			if err != nil {
				return err
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

// 生成死信队列名
func (r *rabbitMQ) generateDlxQueueName(qn QueueName) QueueName {
	return QueueName(fmt.Sprintf("%s_dlx", qn))
}

// 获取绑定到交换机的延迟队列名
func (r *rabbitMQ) getBindExchangeDelayQueueName(queueName QueueName, delay time.Duration) QueueName {
	second := int64(delay / time.Second)
	return QueueName(fmt.Sprintf("%s_%ds_transfer", queueName, second))
}

// 定义绑定到交换机的延迟队列
func (r *rabbitMQ) declareBindExchangeDelayQueue(exchangeName ExchangeName, queueName QueueName, delay time.Duration) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return errors.Wrap(err, "获取通道失败")
	}
	defer func(ch *amqp.Channel) {
		if ch != nil {
			_ = ch.Close()
		}
	}(ch)

	ttl := int64(delay / time.Millisecond)

	_, err = ch.QueueDeclare(string(queueName), true, false, false, false, amqp.Table{
		"x-message-ttl":          ttl, // 消息过期时间，毫秒
		"x-dead-letter-exchange": string(exchangeName),
	})
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("定义延迟队列%s错误", queueName))
	}
	return nil
}

// 定义延迟队列
func (r *rabbitMQ) declareDelayQueue(exchangeName ExchangeName, queueName QueueName, delay time.Duration) (err error) {
	ch, err := r.conn.Channel()
	if err != nil {
		return errors.Wrap(err, "获取通道失败")
	}
	defer func(ch *amqp.Channel) {
		if ch != nil {
			_ = ch.Close()
		}
	}(ch)
	// 计算延迟时间
	ttl := int64(delay / time.Millisecond)

	delayQueueName := r.getDelayQueueName(queueName, delay)

	_, err = ch.QueueDeclare(string(delayQueueName), true, false, false, false, amqp.Table{
		"x-message-ttl":             ttl, // 消息过期时间，毫秒
		"x-dead-letter-exchange":    string(exchangeName),
		"x-dead-letter-routing-key": "",
	})
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("定义延迟队列%s错误", delayQueueName))
	}
	return nil
}

// 获取延迟队列名
func (r *rabbitMQ) getDelayQueueName(queue QueueName, delay time.Duration) QueueName {
	second := int64(delay / time.Second)
	return QueueName(fmt.Sprintf("%s_%ds", queue, second))
}
