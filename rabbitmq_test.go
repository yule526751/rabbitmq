package rabbitmq

import (
	"testing"
	"time"
)

func TestConn(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn("127.0.0.1", 5672, "admin", "123456", "/develop")
	if err != nil {
		t.Error(err)
	}
	defer m.Close()
	t.Log("Conn success")
}

func TestSendExchange(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn("127.0.0.1", 5672, "admin", "123456", "/develop")
	if err != nil {
		t.Error(err)
	}
	defer m.Close()
	t.Log("Conn success")

	if err = m.ExchangeQueueCreate(map[ExchangeName]*Exchange{
		"test_exchange1": {
			Queues: map[QueueName]*Queue{
				"test_queue1": {},
			},
		},
	}); err != nil {
		t.Error(err)
	} else {
		t.Log("ExchangeQueueCreate success")
	}

	if err = m.SendToExchange("test_exchange1", "abc"); err != nil {
		t.Error(err)
	} else {
		t.Log("SendToExchange success")
	}
}

func TestSendDelayQueue(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn("127.0.0.1", 5672, "admin", "123456", "/develop")
	if err != nil {
		t.Error(err)
	}
	defer m.Close()
	t.Log("Conn success")

	if err = m.ExchangeQueueCreate(map[ExchangeName]*Exchange{
		"test_exchange1": {
			Queues: map[QueueName]*Queue{
				"test_queue1": {},
			},
		},
	}); err != nil {
		t.Error(err)
	} else {
		t.Log("ExchangeQueueCreate success")
	}

	if err = m.SendToDelayQueue("test_queue1", 10*time.Second, "abc"); err != nil {
		t.Error(err)
	} else {
		t.Log("SendToDelayQueue success")
	}
}

func TestConsumer(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn("127.0.0.1", 5672, "admin", "123456", "/develop")
	if err != nil {
		t.Error(err)
	}
	defer m.Close()
	t.Log("Conn success")

	if err = m.ExchangeQueueCreate(map[ExchangeName]*Exchange{
		"test_exchange1": {
			Queues: map[QueueName]*Queue{
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
			_ = m.SendToExchange("test_exchange1", "abc")
			//time.Sleep(2 * time.Second)
		}
	}()
	go func() {
		for {
			err = m.RegisterConsumer("test_consumer1", &Consumer{
				QueueName: "test_queue1",
				ConsumeFunc: func(msg []byte) error {
					t.Log(string(msg))
					return nil
				},
			})
			if err != nil {
				return
			}
			err = m.consumerRun("", nil)
			if err != nil {
				return
			}
		}
	}()
	select {}
}
