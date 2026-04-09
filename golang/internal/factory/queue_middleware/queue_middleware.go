package queuemiddleware

import (
	"fmt"
	"log"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type queueMiddleware struct {
	queue         amqp.Queue
	channel       *amqp.Channel
	connection    *amqp.Connection
	stopConsuming chan struct{}
	returnChan    chan amqp.Return
}

const (
	_URL_SIGN = "amqp://guest:guest@%s:%d/"
)

func CreateQueueMiddlewareHelper(
	queueName string,
	connectionSettings m.ConnSettings,
) (m.Middleware, error) {
	conn, err := amqp.Dial(fmt.Sprintf(_URL_SIGN, connectionSettings.Hostname, connectionSettings.Port))
	if err != nil {
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	middleware := queueMiddleware{
		connection:    conn,
		stopConsuming: nil,
	}

	ch, err := conn.Channel()
	if err != nil {
		middleware.Close()
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	middleware.channel = ch

	q, err := ch.QueueDeclare(
		queueName, // name
		true,      // durability
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)
	if err != nil {
		middleware.Close()
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	middleware.queue = q

	returned := ch.NotifyReturn(make(chan amqp.Return, 10))

	go func() {
		for msg := range returned {
			// Aca handleo los mensajes no delivereados hacia el server. Podría aplicar un exp backoff reenviando los mensajes cada cierto tiempo,
			// y si bien entiendo que las go routines son livianas, no quiero mandarme a levantar una go routine por cada una de los mensajes no delivereados.
			// Con un logger a nivel monitoreo por lo menos sos capaz de poder trazar el por qué falla el envio de tus mensajes y a futuro meter un fix
			// sobre eso, de ser necesario.
			log.Printf("Message with id %s not delivered with reply code %d", msg.MessageId, msg.ReplyCode)
		}
	}()

	middleware.returnChan = returned

	return &middleware, nil
}

func (q *queueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {
	if !q.areConnsUp() {
		return m.ErrMessageMiddlewareClose
	}

	q.stopConsuming = make(chan struct{})
	stopCh := q.stopConsuming

	msgs, err := q.channel.Consume(
		q.queue.Name, // queue
		"",           // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}

	connClosed := q.connection.NotifyClose(make(chan *amqp.Error, 1))
	channelClosed := q.channel.NotifyClose(make(chan *amqp.Error, 1))
	// Estos dos channels no los libero porque googleando un poco veo que la convención en Golang es que quien escribe sobre el channel es el owner
	// y el owner es el único que debe cerrar y liberar este recurso. Fuente: https://www.reddit.com/r/golang/comments/1blued8/how_to_check_is_a_channel_is_closed_before/
	// Al fin y al cabo igual entiendo que de llamarse al Close del middleware, se llaman al Close de la connection/channel y por dentro se liberan estos canales justamente.

	for {
		select {
		case d := <-msgs:
			callbackFunc(
				m.Message{Body: string(d.Body)},
				func() { d.Ack(false) },
				func() { d.Nack(false, false) },
			)
		case <-stopCh:
			return nil
		case <-connClosed:
			return m.ErrMessageMiddlewareDisconnected
		case <-channelClosed:
			return m.ErrMessageMiddlewareDisconnected
		}
	}

}

func (q *queueMiddleware) StopConsuming() {
	if !q.areConnsUp() || q.stopConsuming == nil {
		return
	}

	select {
	case <-q.stopConsuming:
	default:
		close(q.stopConsuming)
		q.stopConsuming = nil
	}
}

func (q *queueMiddleware) Send(msg m.Message) (err error) {
	if !q.areConnsUp() {
		return m.ErrMessageMiddlewareDisconnected
	}

	err = q.channel.Publish(
		"",
		q.queue.Name,
		true,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})

	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}
	return nil
}

func (q *queueMiddleware) Close() error {
	var err error
	if q.channel != nil {
		err = q.channel.Close()
		if err != nil {
			return err
		}
	}
	if q.connection != nil {
		err = q.connection.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// Helpers

func (q *queueMiddleware) areConnsUp() bool {
	return q.connection != nil && q.channel != nil && !q.connection.IsClosed() && !q.channel.IsClosed()
}
