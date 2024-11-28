CREATE TABLE `rabbitmq_msg` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `exchange_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '交换机名',
  `queue_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '队列名',
  `routing_key` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '路由键',
  `msg` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci COMMENT '消息',
  `delay` bigint unsigned DEFAULT NULL COMMENT '延迟时间，秒',
  `created_at` datetime NOT NULL,
  `updated_at` datetime NOT NULL,
  `deleted_at` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='rabbitmq消息';