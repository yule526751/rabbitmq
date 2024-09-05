package rabbitmq

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ConsumeFunc func(msg []byte) error

type Consumer struct {
	QueueName   QueueName   // 队列名称
	ConsumeFunc ConsumeFunc // 消费函数
}

func (r *rabbitMQ) RegisterConsumer(consumerName string, consumer *Consumer) error {
	_, ok := r.consumes[consumerName]
	if ok {
		return errors.New(fmt.Sprintf("消费者 %s 已存在，注册失败", consumerName))
	}

	r.consumesRegisterLock.Lock()
	defer r.consumesRegisterLock.Unlock()
	r.consumes[consumerName] = struct{}{}
	if r.openLog {
		log.Printf("consumer %s register success\n", consumerName)
	}
	return r.consumerRun(consumerName, consumer)
}

func (r *rabbitMQ) consumerRun(consumerName string, consumer *Consumer) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("%s获取信道失败", consumerName))
	}

	err = ch.Qos(1, 0, false)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("%s设置消息投递模式失败", consumerName))
	}

	var msgChan <-chan amqp.Delivery
	msgChan, err = ch.Consume(string(consumer.QueueName), "", false, false, false, false, nil)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("队列 %s 消费失败", consumer.QueueName))
	}

	// handle 处理逻辑
	go r.handle(ch, consumer, msgChan)

	if r.openLog {
		log.Printf("consumer %s listen queue %s run....\n", consumerName, consumer.QueueName)
	}
	return nil
}

// handle 处理逻辑
func (r *rabbitMQ) handle(ch *amqp.Channel, consumer *Consumer, msgChan <-chan amqp.Delivery) {
	for msg := range msgChan {
		_, errStr := r.done(consumer.ConsumeFunc, msg.Body)
		if errStr != "" {
			m := make(map[string]interface{})
			// 解析json，添加错误信息和错误时间
			err := json.Unmarshal(msg.Body, &m)
			if err != nil {
				if r.openLog {
					log.Printf("parse json error: %+v", err)
				}
				continue
			}
			// 添加错误和时间
			m["dlx_err"] = errStr
			m["dlx_at"] = time.Now().Local().Format(time.DateTime)

			// 发送到死信队列
			body, err := json.Marshal(m)
			if err != nil {
				if r.openLog {
					log.Printf("parse json error: %+v", err)
				}
				continue
			}
			dlxQueueName := r.generateDlxQueueName(consumer.QueueName)
			err = ch.Publish("", string(dlxQueueName), false, false, amqp.Publishing{ContentType: "text/plain", Body: body})
			if err != nil {
				if r.openLog {
					log.Printf("send %s error: %+v", dlxQueueName, err)
				}
			}
		}

		for i := 0; i < r.ackRetryTime; i++ {
			if r.conn.IsClosed() {
				err := r.reConn()
				if err != nil {
					if r.openLog {
						log.Printf("重连rabbitmq失败：%+v", err)
					}
					continue
				}
				ch, err = r.conn.Channel()
				if err != nil {
					if r.openLog {
						log.Printf("获取信道失败：%+v", err)
					}

					continue
				}
			}
			err := ch.Ack(msg.DeliveryTag, false)
			if err != nil {
				if r.openLog {
					log.Printf("ack error: %+v", err)
				}
			} else {
				break
			}
		}
	}
}

func (r *rabbitMQ) done(consumeFunc ConsumeFunc, msg []byte) (isPanic bool, errStr string) {
	var err error
	defer func() {
		if err := recover(); err != nil {
			isPanic = true
			errStr = fmt.Sprintf("%+v", err)
		}
	}()
	err = consumeFunc(msg)
	if err != nil {
		errStr = fmt.Sprintf("%+v", err)
	}
	return
}
