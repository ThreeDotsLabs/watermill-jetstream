package jetstream

import (
	"bytes"
	"encoding/gob"

	nats "github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message"
)

type Marshaler interface {
	Marshal(topic string, msg *message.Message) ([]byte, error)
}

type Unmarshaler interface {
	Unmarshal(*nats.Msg) (*message.Message, error)
}

type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}

// GobMarshaler is marshaller which is using Gob to marshal Watermill messages.
type GobMarshaler struct{}

func (GobMarshaler) Marshal(topic string, msg *message.Message) ([]byte, error) {
	// todo - use pool
	buf := new(bytes.Buffer)

	encoder := gob.NewEncoder(buf)
	if err := encoder.Encode(msg); err != nil {
		return nil, errors.Wrap(err, "cannot encode message")
	}

	return buf.Bytes(), nil
}

func (GobMarshaler) Unmarshal(natsMsg *nats.Msg) (*message.Message, error) {
	// todo - use pool
	buf := new(bytes.Buffer)

	_, err := buf.Write(natsMsg.Data)
	if err != nil {
		return nil, errors.Wrap(err, "cannot write nats message data to buffer")
	}

	decoder := gob.NewDecoder(buf)

	var decodedMsg message.Message
	if err := decoder.Decode(&decodedMsg); err != nil {
		return nil, errors.Wrap(err, "cannot decode message")
	}

	// creating clean message, to avoid invalid internal state with ack
	msg := message.NewMessage(decodedMsg.UUID, decodedMsg.Payload)
	msg.Metadata = decodedMsg.Metadata

	return msg, nil
}
