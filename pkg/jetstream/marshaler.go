package jetstream

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

type Marshaler interface {
	// Marshal transforms a watermill message into binary format.
	Marshal(topic string, msg *message.Message) (*nats.Msg, error)
}

type Unmarshaler interface {
	// Unmarshal extracts a watermill message from a nats message.
	Unmarshal(*nats.Msg) (*message.Message, error)
}

type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}

func defaultNatsMsg(topic string, uuid string, data []byte) *nats.Msg {
	return &nats.Msg{
		Subject: subject(topic, uuid),
		Data:    data,
	}
}

func subject(topic string, uuid string) string {
	return fmt.Sprintf("%s.%s", topic, uuid)
}

// GobMarshaler is marshaller which is using Gob to marshal Watermill messages.
type GobMarshaler struct{}

// Marshal transforms a watermill message into gob format.
func (GobMarshaler) Marshal(topic string, msg *message.Message) (*nats.Msg, error) {
	buf := new(bytes.Buffer)

	encoder := gob.NewEncoder(buf)
	if err := encoder.Encode(msg); err != nil {
		return nil, errors.Wrap(err, "cannot encode message")
	}

	return defaultNatsMsg(topic, msg.UUID, buf.Bytes()), nil
}

// Unmarshal extracts a watermill message from a nats message.
func (GobMarshaler) Unmarshal(natsMsg *nats.Msg) (*message.Message, error) {
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

// JSONMarshaler uses encoding/json to marshal Watermill messages.
type JSONMarshaler struct{}

// Marshal transforms a watermill message into JSON format.
func (JSONMarshaler) Marshal(topic string, msg *message.Message) (*nats.Msg, error) {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil, errors.Wrap(err, "cannot encode message")
	}

	return defaultNatsMsg(topic, msg.UUID, bytes), nil
}

// Unmarshal extracts a watermill message from a nats message.
func (JSONMarshaler) Unmarshal(natsMsg *nats.Msg) (*message.Message, error) {
	var decodedMsg message.Message
	err := json.Unmarshal(natsMsg.Data, &decodedMsg)
	if err != nil {
		return nil, errors.Wrap(err, "cannot decode message")
	}

	// creating clean message, to avoid invalid internal state with ack
	msg := message.NewMessage(decodedMsg.UUID, decodedMsg.Payload)
	msg.Metadata = decodedMsg.Metadata

	return msg, nil
}
