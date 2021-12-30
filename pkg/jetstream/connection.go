package jetstream

import nats "github.com/nats-io/nats.go"

type NatsConnConfig struct {
	// URL is the NATS URL.
	URL string

	// NatsOptions are custom []nats.Option passed to the connection.
	// It is also used to provide connection parameters, for example:
	// 		nats.Name("jetstream")
	NatsOptions []nats.Option
}

func NewJetstreamConnection(config *NatsConnConfig) (nats.JetStreamContext, error) {
	nc, err := NewNatsConnection(config)
	if err != nil {
		return nil, err
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	return js, nil
}

func NewNatsConnection(config *NatsConnConfig) (*nats.Conn, error) {
	return nats.Connect(config.URL, config.NatsOptions...)
}
