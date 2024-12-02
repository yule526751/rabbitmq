package rabbitmq

import (
	"encoding/json"
	"github.com/shopspring/decimal"
	"github.com/yule526751/rabbitmq/models"
	"reflect"
	"time"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

// 发送消息到交换机
func (r *rabbitMQ) SendToExchange(exchangeName ExchangeName, msg interface{}, routingKey ...string) (err error) {
	if exchangeName == "" {
		return errors.New("交换机不能为空")
	}

	var rk string
	if routingKey != nil && len(routingKey) > 0 {
		rk = routingKey[0]
	}

	// 直接发送
	return r.send(&sendReq{
		Exchange:   exchangeName,
		RoutingKey: rk,
		Msg:        msg,
	})
}

// 发送消息到交换机
func (r *rabbitMQ) SendToExchangeTx(f func(data *models.RabbitmqMsg) error, exchangeName ExchangeName, msg interface{}, routingKey ...string) (err error) {
	if exchangeName == "" {
		return errors.New("交换机不能为空")
	}

	var rk string
	if routingKey != nil && len(routingKey) > 0 {
		rk = routingKey[0]
	}
	// 断言消息类型
	body, err := r.convertMsg(msg)
	if err != nil {
		return err
	}
	err = f(&models.RabbitmqMsg{
		ExchangeName: string(exchangeName),
		Msg:          body,
		RoutingKey:   rk,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	})
	if err != nil {
		return errors.New("创建队列消息记录失败")
	}
	return nil
}

// 发送消息到指定队列，不是交换机
func (r *rabbitMQ) SendToQueue(queueName QueueName, msg interface{}) error {
	// 检查参数
	if queueName == "" {
		return errors.New("队列不能为空")
	}
	// 直接发送
	return r.send(&sendReq{
		RoutingKey: string(queueName),
		Msg:        msg,
	})
}

// 发送消息到指定队列，不是交换机
func (r *rabbitMQ) SendToQueueTx(f func(data *models.RabbitmqMsg) error, queueName QueueName, msg interface{}) error {
	// 检查参数
	if queueName == "" {
		return errors.New("队列不能为空")
	}
	// 断言消息类型
	body, err := r.convertMsg(msg)
	if err != nil {
		return err
	}
	err = f(&models.RabbitmqMsg{
		Msg:        body,
		RoutingKey: string(queueName),
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	})
	if err != nil {
		return errors.New("创建队列消息记录失败")
	}
	return nil
}

// 发送延迟消息到指定队列，不是交换机
func (r *rabbitMQ) SendToQueueDelay(queueName QueueName, delay time.Duration, msg interface{}) error {
	// 检查参数
	if queueName == "" {
		return errors.New("队列不能为空")
	}
	if delay <= time.Second {
		return errors.New("延迟时间必须大于等于1秒")
	}

	_, exist := r.queueDelayMap[queueName][delay]

	if !exist {
		// 自动创建不存在的延迟队列
		err := r.declareDelayQueue(queueName, delay)
		if err != nil {
			return err
		}
		if _, ok := r.queueDelayMap[queueName]; !ok {
			r.queueDelayMap[queueName] = make(map[time.Duration]struct{})
		}
		r.queueDelayMap[queueName][delay] = struct{}{}
	}

	// 直接发送
	return r.send(&sendReq{
		Queue: queueName,
		Msg:   msg,
		Delay: delay,
	})
}

// 发送延迟消息到指定队列，不是交换机
func (r *rabbitMQ) SendToQueueDelayTx(f func(data *models.RabbitmqMsg) error, queueName QueueName, delay time.Duration, msg interface{}) error {
	// 检查参数
	if queueName == "" {
		return errors.New("队列不能为空")
	}
	if delay <= time.Second {
		return errors.New("延迟时间必须大于等于1秒")
	}

	_, exist := r.queueDelayMap[queueName][delay]
	if !exist {
		// 自动创建不存在的延迟队列
		err := r.declareDelayQueue(queueName, delay)
		if err != nil {
			return err
		}
		if _, ok := r.queueDelayMap[queueName]; !ok {
			r.queueDelayMap[queueName] = make(map[time.Duration]struct{})
		}
		r.queueDelayMap[queueName][delay] = struct{}{}
	}
	// 断言消息类型
	body, err := r.convertMsg(msg)
	if err != nil {
		return err
	}

	d := decimal.NewFromInt(int64(delay)).Div(decimal.NewFromInt(int64(time.Second))).IntPart()
	err = f(&models.RabbitmqMsg{
		QueueName: string(queueName),
		Msg:       body,
		Delay:     uint64(d),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	})
	if err != nil {
		return errors.New("创建队列消息记录失败")
	}
	return nil
}

func (r *rabbitMQ) convertMsg(msg interface{}) (data []byte, err error) {
	ref := reflect.TypeOf(msg)
	for ref.Kind() == reflect.Ptr {
		ref = ref.Elem()
	}
	switch ref.Kind() {
	case reflect.Struct, reflect.Map:
		// 结构体，map，转json
		data, err = json.Marshal(msg)
		if err != nil {
			return nil, errors.Wrap(err, "消息序列json化失败")
		}
	case reflect.Slice:
		if ref.Elem().Kind() == reflect.Uint8 {
			return msg.([]byte), nil
		}
		return nil, errors.New("消息类型只支持结构体和map")
	default:
		// 其他转字符串
		return nil, errors.New("消息类型只支持结构体和map")
	}
	return data, nil
}

type sendReq struct {
	Exchange   ExchangeName  // 交换机
	Queue      QueueName     // 队列名
	RoutingKey string        // 路由
	Msg        interface{}   // 数据
	Delay      time.Duration // 延迟时间
}

// 发送消息
// 交换机和路由都为空，用队列名做路由发消息给队列
func (r *rabbitMQ) send(req *sendReq) error {
	// 断言消息类型
	body, err := r.convertMsg(req.Msg)
	if err != nil {
		return err
	}

	if r.conn.IsClosed() {
		err = r.reConn()
		if err != nil {
			return err
		}
	}
	ch, err := r.conn.Channel()
	if err != nil {
		return errors.Wrap(err, "获取mq通道失败")
	}
	defer func(ch *amqp.Channel) {
		_ = ch.Close()
	}(ch)

	// 使用事务模式
	err = ch.Tx()
	if err != nil {
		return errors.Wrap(err, "开启mq事务模式失败")
	}

	// 延时队列的消息只通过路由发送到队列
	if req.Delay > 0 {
		req.Exchange = ""
		req.RoutingKey = string(r.getDelayQueueName(req.Queue, req.Delay))
	}

	err = ch.Publish(string(req.Exchange), req.RoutingKey, false, false, amqp.Publishing{
		ContentType:  "text/plain",
		Body:         body,
		DeliveryMode: 2, // 持久化消息
	})
	if err != nil {
		_ = ch.TxRollback()
		return errors.Wrap(err, "消息发送失败")
	}

	err = ch.TxCommit()
	if err != nil {
		return errors.Wrap(err, "提交mq事务失败")
	}

	return nil
}
