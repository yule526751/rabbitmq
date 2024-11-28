package models

import (
	"gorm.io/gorm"
)

type RabbitmqMsg struct {
	gorm.Model
	ExchangeName string `gorm:"column:exchange_name"` // 交换机名称
	QueueName    string `gorm:"column:queue_name"`    // 队列名称
	RoutingKey   string `gorm:"column:routing_key"`   // 路由键
	Msg          []byte `gorm:"column:msg"`           // 消息
	Delay        uint64 `gorm:"column:delay"`         // 延迟时间,秒
}
