package rabbitmq

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/yule526751/rabbitmq/models"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"testing"
	"time"
)

var (
	rabbitmqHost      = "127.0.0.1"
	rabbitmqPort      = 5672
	rabbitmqUser      = "admin"
	rabbitmqPassword  = "123456"
	rabbitmqVhost     = "/develop"
	mysqlHost         = "127.0.0.1"
	mysqlPort         = "3306"
	mysqlUsername     = "root"
	mysqlPassword     = "123456"
	mysqlDatabase     = ""
	mysqlMaxIdleConns = 10
	mysqlMaxOpenConns = 50
)

func TestConn(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, "develop")
	if err != nil {
		t.Error(err)
	}
	defer m.Close()
	t.Log("Conn success")
}

func TestSendExchange(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
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

	if err = m.SendToExchange("test_exchange1", map[string]interface{}{
		"id": 1,
	}); err != nil {
		t.Error(err)
	} else {
		t.Log("SendToExchange success")
	}
}

func TestSentExchangeTX(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
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
	initMysql()
	err = Mysql.Transaction(func(tx *gorm.DB) error {
		return m.SendToExchangeTx(func(data *models.RabbitmqMsg) error {
			return tx.Model(&models.RabbitmqMsg{}).Create(data).Error
		}, "test_exchange1", map[string]interface{}{"id": 1})
	})
	if err != nil {
		t.Error(err)
	}
}

func TestSendDelayQueue(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
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

	if err = m.SendToQueueDelay("test_queue1", 10*time.Second, map[string]interface{}{"id": 1}); err != nil {
		t.Error(err)
	} else {
		t.Log("SendToQueueDelay success")
	}
}

func TestSendDelayQueueTx(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
	if err != nil {
		t.Error(err)
	}
	defer m.Close()
	t.Log("Conn success")

	initMysql()
	err = Mysql.Transaction(func(tx *gorm.DB) error {
		return m.SendToQueueDelayTx(func(data *models.RabbitmqMsg) error {
			return tx.Model(&models.RabbitmqMsg{}).Create(data).Error
		}, "test_queue1", 10*time.Second, map[string]interface{}{"id": 1})
	})
	if err != nil {
		t.Error(err)
	}
}

func TestConsumer(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
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
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
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
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
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
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
	if err != nil {
		t.Error(err)
	}
	defer func(m *rabbitMQ) {
		_ = m.Close()
	}(m)
	t.Log("Conn success")
	err = m.SendToQueue("test_queue1", map[string]interface{}{
		"id": 1,
	})
	t.Log(err)
}

func TestSendToQueueTx(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
	if err != nil {
		t.Error(err)
	}
	defer func(m *rabbitMQ) {
		_ = m.Close()
	}(m)
	t.Log("Conn success")
	initMysql()
	err = Mysql.Transaction(func(tx *gorm.DB) error {
		return m.SendToQueueTx(func(data *models.RabbitmqMsg) error {
			return tx.Model(&models.RabbitmqMsg{}).Create(data).Error
		}, "test_queue1", map[string]interface{}{"id": 1})
	})
	if err != nil {
		t.Error(err)
	}
	t.Log(err)
}

func TestCirculateSendMsg(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
	if err != nil {
		t.Error(err)
	}
	defer func(m *rabbitMQ) {
		_ = m.Close()
	}(m)
	t.Log("Conn success")
	initMysql()
	m.CirculateSendMsg(context.Background(), Mysql)
}

var Mysql *gorm.DB

func initMysql() {
	var err error
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		mysqlUsername,
		mysqlPassword,
		mysqlHost,
		mysqlPort,
		mysqlDatabase,
	)
	c := &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
		},
	}

	c.Logger = logger.Default.LogMode(logger.Info)

	Mysql, err = gorm.Open(mysql.New(mysql.Config{
		DSN:                      dsn,
		DisableDatetimePrecision: true, // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
	}), c)
	if err != nil {
		panic(err)
	}

	// 设置连接池
	var db *sql.DB
	db, err = Mysql.DB()
	if err != nil {
		return
	}
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(2 * time.Minute)
	db.SetMaxIdleConns(mysqlMaxIdleConns)
	db.SetMaxOpenConns(mysqlMaxOpenConns)
}
