package rabbitmq

import (
	"fmt"
	"testing"
	"time"
)

func TestConn(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn("127.0.0.1", 5672, "admin", "123456", "/")
	if err != nil {
		t.Error(err)
	}
	defer m.Close()
	t.Log("Conn success")
}

func TestSendExchange(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn("127.0.0.1", 5672, "admin", "123456", "/")
	if err != nil {
		t.Error(err)
	}
	defer m.Close()
	t.Log("Conn success")

	// if err = m.ExchangeQueueCreate(map[ExchangeName]*Exchange{
	// 	"test_exchange1": {
	// 		BindQueues: map[QueueName]*Queue{
	// 			"test_queue1": {},
	// 		},
	// 	},
	// }); err != nil {
	// 	t.Error(err)
	// } else {
	// 	t.Log("ExchangeQueueCreate success")
	// }

	if err = m.SendToExchange("test_exchange1", map[string]interface{}{
		"id": 1,
	}); err != nil {
		t.Error(err)
	} else {
		t.Log("SendToExchange success")
	}
}

func TestSendDelayQueue(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn("127.0.0.1", 5672, "admin", "123456", "/")
	if err != nil {
		t.Error(err)
	}
	defer m.Close()
	t.Log("Conn success")

	if err = m.ExchangeQueueCreate(map[ExchangeName]*Exchange{
		"test_exchange1": {
			BindQueues: map[QueueName]*Queue{
				"test_queue1": {},
			},
		},
	}); err != nil {
		t.Error(err)
	} else {
		t.Log("ExchangeQueueCreate success")
	}

	if err = m.SendToQueueDelay("test_queue1", 10*time.Second, "abc"); err != nil {
		t.Error(err)
	} else {
		t.Log("SendToQueueDelay success")
	}
}

func TestConsumer(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn("127.0.0.1", 5672, "admin", "123456", "/develop")
	if err != nil {
		t.Error(err)
	}
	defer func(m *rabbitMQ) {
		_ = m.Close()
	}(m)
	t.Log("Conn success")

	if err = m.ExchangeQueueCreate(map[ExchangeName]*Exchange{
		"test_exchange1": {
			BindQueues: map[QueueName]*Queue{
				"test_queue1": {},
			},
		},
	}); err != nil {
		t.Error(err)
	} else {
		t.Log("ExchangeQueueCreate success")
	}

	go func() {
		for {
			err = m.SendToExchange("test_exchange1", map[string]interface{}{
				"id": 1,
			})
			t.Log("send abc", err, time.Now())
			time.Sleep(2 * time.Second)
		}
	}()
	go func() {
		select {
		case err = <-m.notifyClose:
			t.Log(err, 1231241241)
			if m.conn.IsClosed() {
				_ = m.reConn()
			}
		}
	}()
	go func() {
		_ = m.RegisterConsumer("test_consumer1", &Consumer{
			QueueName:   "test_queue1",
			ConsumeFunc: handle,
		})
	}()
	select {}
}

func handle(data []byte) error {
	fmt.Println(string(data), time.Now())
	return nil
}

func TestBingDelayQueue(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn("127.0.0.1", 5672, "admin", "123456", "/develop")
	if err != nil {
		t.Error(err)
	}
	defer func(m *rabbitMQ) {
		_ = m.Close()
	}(m)
	t.Log("Conn success")
	m.ExchangeQueueCreate(map[ExchangeName]*Exchange{
		"test_exchange1": {
			BindQueues: map[QueueName]*Queue{
				"test_queue1": {},
			},
		},
		"test_exchange2": {
			BindQueues: map[QueueName]*Queue{
				"test_queue2": {},
			},
		},
		"test_exchange3": {
			BindQueues: map[QueueName]*Queue{
				"test_queue3": {},
			},
		},
	})
	if err = m.BindDelayQueueToExchange("test_exchange1", "test_exchange2", 20*time.Second); err != nil {
		t.Error(err)
	} else {
		t.Log("BindDelayQueueToExchange success")
	}
	if err = m.BindDelayQueueToExchange("test_exchange1", "test_exchange3", 40*time.Second); err != nil {
		t.Error(err)
	} else {
		t.Log("BindDelayQueueToExchange success")
	}
}

func TestSendToDelayQueue(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn("127.0.0.1", 5672, "admin", "123456", "/develop")
	if err != nil {
		t.Error(err)
	}
	defer func(m *rabbitMQ) {
		_ = m.Close()
	}(m)
	t.Log("Conn success")
	err = m.SendToQueueDelay("test_queue2", 20*time.Second, map[string]interface{}{
		"id": 1,
	})
	t.Log(err)
	err = m.SendToQueueDelay("test_queue2", 20*time.Second, map[string]interface{}{
		"id": 1,
	})
}

func TestSendToQueue(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn("127.0.0.1", 5672, "admin", "123456", "/develop")
	if err != nil {
		t.Error(err)
	}
	defer func(m *rabbitMQ) {
		_ = m.Close()
	}(m)
	t.Log("Conn success")
	err = m.SendToQueue("test_queue2", map[string]interface{}{
		"id": 1,
	})
	t.Log(err)
}
