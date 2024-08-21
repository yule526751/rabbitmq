package rabbitmq

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
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
	r.consumes[consumerName] = consumer
	log.Printf("consumer %s register success\n", consumerName)
	err := r.consumerRun(consumerName, consumer)
	if err != nil {
		return err
	}
	return nil
}

func (r *rabbitMQ) consumerRun(consumerName string, consumer *Consumer) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return errors.Wrap(err, "获取channel失败")
	}

	var msgChan <-chan amqp.Delivery
	msgChan, err = ch.Consume(string(consumer.QueueName), "", false, false, false, false, nil)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("队列 %s 消费失败", consumer.QueueName))
	}

	// handle 处理逻辑
	go r.handle(consumer, msgChan)

	log.Printf("consumer %s listen queue %s run....\n", consumerName, consumer.QueueName)
	return nil
}

// handle 处理逻辑
func (r *rabbitMQ) handle(consumer *Consumer, msgChan <-chan amqp.Delivery) {
	defer func() {
		if err := recover(); err != nil {
		}
	}()

	for msg := range msgChan {
		err := consumer.ConsumeFunc(msg.Body)
		if err != nil {
			m := make(map[string]interface{})
			// 解析json，添加错误信息和错误时间
			err = json.Unmarshal(msg.Body, &m)
			if err != nil {
				log.Printf("parse json error: %+v", err)
				continue
			}
			// 添加错误和时间
			m["dlx_err"] = fmt.Sprintf("%+v", err)
			m["dlx_at"] = time.Now().Local().Format(time.DateTime)

			// 发送到死信队列
			body, _ := json.Marshal(m)
			ch, err := r.conn.Channel()
			if err != nil {
				return
			}
			dlxQueueName := r.generateDlxQueueName(consumer.QueueName)
			err = ch.Publish("", string(dlxQueueName), false, false, amqp.Publishing{ContentType: "text/plain", Body: body})
			if err != nil {
			}
		}

		channel, err := r.conn.Channel()
		if err != nil {
			return
		}
		channel.Ack(msg.DeliveryTag, false)
		err = msg.Ack(false)
		if err != nil {
			log.Printf("ack error: %+v", err)
		}
	}
}