package models

import (
	"time"
)

type RabbitmqMsg struct {
	ID           uint       `gorm:"primarykey" json:"id"`
	ExchangeName string     `gorm:"column:exchange_name" json:"exchange_name"` // 交换机名称
	QueueName    string     `gorm:"column:queue_name" json:"queue_name"`       // 队列名称
	RoutingKey   string     `gorm:"column:routing_key" json:"routing_key"`     // 路由键
	Msg          []byte     `gorm:"column:msg" json:"msg"`                     // 消息
	Delay        uint64     `gorm:"column:delay" json:"delay"`                 // 延迟时间,秒
	CreatedAt    time.Time  `gorm:"column:created_at" json:"created_at"`
	UpdatedAt    time.Time  `gorm:"column:updated_at" json:"updated_at"`
	DeletedAt    *time.Time `gorm:"column:deleted_at;index" json:"deleted_at"`
}
