package factory

import (
	em "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory/exchange_middleware"
	qm "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory/queue_middleware"
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	return qm.CreateQueueMiddlewareHelper(
		queueName,
		connectionSettings,
	)
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	return em.CreateExchangeMiddlewareHelper(
		exchange,
		keys,
		connectionSettings,
	)
}
