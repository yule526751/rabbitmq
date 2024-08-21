package rabbitmq

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"reflect"
	"time"
)

// 发送消息到交换机
func (r *rabbitMQ) SendToExchange(exchangeName ExchangeName, msg interface{}, routingKey ...string) (err error) {
	if exchangeName == "" {
		return errors.New("交换机不能为空")
	}
	// 检查参数
	if _, ok := r.exchangeMap[exchangeName]; !ok {
		return errors.New(string("请先定义交换机" + exchangeName))
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

// 发送延迟消息
func (r *rabbitMQ) SendToDelayQueue(queueName QueueName, delay time.Duration, msg interface{}) error {
	// 检查参数
	if queueName == "" {
		return errors.New("队列不能为空")
	}
	if delay <= time.Second {
		return errors.New("延迟时间必须大于等于1秒")
	}

	// 发给延迟队列要检查是否已定义队列和绑定交换机
	exchangeName, ok := r.queueExchangeMap[queueName]
	if !ok {
		return errors.New(string("请先定义队列" + queueName))
	}

	_, exist := r.queueDelayMap[queueName][delay]

	if !exist {
		// 自动创建不存在的延迟队列
		ch, err := r.conn.Channel()
		if err != nil {
			return errors.Wrap(err, "获取mq通道失败")
		}
		defer func(ch *amqp.Channel) {
			if ch == nil {
				_ = ch.Close()
			}
		}(ch)
		err = r.declareDelayQueue(exchangeName, queueName, delay)
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
func (r *rabbitMQ) convertMsg(msg interface{}) (data []byte, err error) {
	ref := reflect.TypeOf(msg)
	for ref.Kind() == reflect.Ptr {
		ref = ref.Elem()
	}
	switch ref.Kind() {
	case reflect.Struct, reflect.Map, reflect.Slice, reflect.Array:
		// 结构体，map，数组转json
		data, err = json.Marshal(msg)
		if err != nil {
			return nil, errors.Wrap(err, "消息序列json化失败")
		}
	default:
		// 其他转字符串
		data = []byte(fmt.Sprintf("%s", msg))
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

	ch, err := r.conn.Channel()
	if err != nil {
		return errors.Wrap(err, "获取mq通道失败")
	}
	defer func(ch *amqp.Channel) {
		if ch == nil {
			_ = ch.Close()
		}
	}(ch)

	for i := 0; i < r.sendRetryTime; i++ {
		// 使用事务模式
		err = ch.Tx()
		if err != nil {
			err = errors.Wrap(err, "开启mq事务模式失败")
			continue
		}

		// 延时队列的消息只通过路由发送到队列
		if req.Delay > 0 {
			req.Exchange = ""
			req.RoutingKey = string(r.getDelayQueueName(req.Queue, req.Delay))
		}

		err = ch.Publish(string(req.Exchange), req.RoutingKey, false, false, amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})
		if err != nil {
			_ = ch.TxRollback()
			err = errors.Wrap(err, "消息发送失败")
			continue
		}

		err = ch.TxCommit()
		if err != nil {
			err = errors.Wrap(err, "提交mq事务失败")
			continue
		}
		break
	}

	return err
}
