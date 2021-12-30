package jetstream

import (
	"fmt"
	nats "github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type PublisherConfig struct {
	// URL is the NATS URL.
	URL string

	// NatsOptions are custom options for a connection.
	NatsOptions []nats.Option

	// Marshaler is marshaler used to marshal messages to stan format.
	Marshaler Marshaler
}

type PublisherPublishConfig struct {
	// Marshaler is marshaler used to marshal messages to stan format.
	Marshaler Marshaler
}

func (c PublisherConfig) Validate() error {
	if c.Marshaler == nil {
		return errors.New("PublisherConfig.Marshaler is missing")
	}

	return nil
}

func (c PublisherConfig) GetPublisherPublishConfig() PublisherPublishConfig {
	return PublisherPublishConfig{
		Marshaler: c.Marshaler,
	}
}

type Publisher struct {
	conn   *nats.Conn
	config PublisherPublishConfig
	logger watermill.LoggerAdapter
	js     nats.JetStreamContext
}

// NewPublisher creates a new Publisher.
func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	conn, err := nats.Connect(config.URL, config.NatsOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to nats")
	}

	return NewPublisherWithNatsConn(conn, config.GetPublisherPublishConfig(), logger)
}

func NewPublisherWithNatsConn(conn *nats.Conn, config PublisherPublishConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	js, err := conn.JetStream()

	if err != nil {
		return nil, err
	}

	return &Publisher{
		conn:   conn,
		config: config,
		logger: logger,
		js:     js,
	}, nil
}

// Publish publishes message to NATS.
//
// Publish will not return until an ack has been received from NATS Streaming.
// When one of messages delivery fails - function is interrupted.
func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	err := initStream(p.js, topic)

	if err != nil {
		return err
	}

	for _, msg := range messages {
		messageFields := watermill.LogFields{
			"message_uuid": msg.UUID,
			"topic_name":   topic,
		}

		p.logger.Trace("Publishing message", messageFields)

		b, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return err
		}

		if _, err := p.js.Publish(fmt.Sprintf("%s.%s", topic, msg.UUID), b); err != nil {
			return errors.Wrap(err, "sending message failed")
		}
	}

	return nil
}

func (p *Publisher) Close() error {
	p.logger.Trace("Closing publisher", nil)
	defer p.logger.Trace("Publisher closed", nil)

	p.conn.Close()

	return nil
}
