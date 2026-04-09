package exchangemiddleware

import (
	"fmt"
	"log"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type exchangeMiddleware struct {
	queue         amqp.Queue
	exchange      string
	publishKeys   []string
	channel       *amqp.Channel
	connection    *amqp.Connection
	stopConsuming chan struct{}
	returnChan    chan amqp.Return
}

const (
	_URL_SIGN = "amqp://guest:guest@%s:%d/"
)

func CreateExchangeMiddlewareHelper(
	exchange string,
	keys []string,
	connectionSettings m.ConnSettings,
) (m.Middleware, error) {
	conn, err := amqp.Dial(fmt.Sprintf(_URL_SIGN, connectionSettings.Hostname, connectionSettings.Port))
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchange,            // name
		amqp.ExchangeDirect, // type
		false,               // durability
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}
	q, err := ch.QueueDeclare(
		"",    // name
		false, // durability
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}

	for _, key := range keys {
		err = ch.QueueBind(
			q.Name,   // queue name
			key,      // routing key
			exchange, // exchange
			false,
			nil,
		)
		if err != nil {
			ch.Close()
			conn.Close()
			return nil, m.ErrMessageMiddlewareMessage
		}
	}

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

	return &exchangeMiddleware{
		queue:         q,
		exchange:      exchange,
		publishKeys:   keys,
		channel:       ch,
		connection:    conn,
		stopConsuming: nil,
		returnChan:    returned,
	}, nil
}

func (e *exchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {
	if !e.areConnsUp() {
		return m.ErrMessageMiddlewareClose
	}

	e.stopConsuming = make(chan struct{})
	stopCh := e.stopConsuming

	msgs, err := e.channel.Consume(
		e.queue.Name, // queue
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

	connClosed := e.connection.NotifyClose(make(chan *amqp.Error, 1))
	channelClosed := e.channel.NotifyClose(make(chan *amqp.Error, 1))
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

func (e *exchangeMiddleware) StopConsuming() {
	if !e.areConnsUp() || e.stopConsuming == nil {
		return
	}

	select {
	case <-e.stopConsuming:
	default:
		close(e.stopConsuming)
		e.stopConsuming = nil
	}
}

func (e *exchangeMiddleware) Send(msg m.Message) (err error) {
	if !e.areConnsUp() {
		return m.ErrMessageMiddlewareDisconnected
	}

	for _, key := range e.publishKeys {
		err = e.channel.Publish(
			e.exchange,
			key,
			true,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			})

		if err != nil {
			return m.ErrMessageMiddlewareMessage
		}
	}

	return nil
}

func (e *exchangeMiddleware) Close() error {
	var err error

	if e.channel != nil {
		err = e.channel.Close()
		if err != nil {
			return err
		}
	}
	if e.connection != nil {
		err = e.connection.Close()
		if err != nil {
			return err
		}
	}

	return err
}

// Helpers

func (e *exchangeMiddleware) areConnsUp() bool {
	return e.connection != nil && e.channel != nil && !e.connection.IsClosed() && !e.channel.IsClosed()
}
