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

func defaultNatsMsg(topic string, uuid string, data []byte, hdr nats.Header) *nats.Msg {
	return &nats.Msg{
		Subject: subject(topic, uuid),
		Data:    data,
		Header:  hdr,
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

	return defaultNatsMsg(topic, msg.UUID, buf.Bytes(), nil), nil
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

	return defaultNatsMsg(topic, msg.UUID, bytes, nil), nil
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

// NATSMarshaler uses NATS header to marshal directly between watermill and NATS formats.
// The watermill UUID is stored at _watermill_message_uuid
type NATSMarshaler struct{}

// can't use nats.MsgIdHeader because that opts into deduplication
const watermillUUID = "_watermill_message_uuid"

// Marshal transforms a watermill message into JSON format.
func (*NATSMarshaler) Marshal(topic string, msg *message.Message) (*nats.Msg, error) {
	header := make(nats.Header)

	header.Set(watermillUUID, msg.UUID)

	for k, v := range msg.Metadata {
		header.Set(k, v)
	}

	data := msg.Payload
	id := msg.UUID

	return defaultNatsMsg(topic, id, data, header), nil
}

// Unmarshal extracts a watermill message from a nats message.
func (*NATSMarshaler) Unmarshal(natsMsg *nats.Msg) (*message.Message, error) {
	data := natsMsg.Data

	hdr := natsMsg.Header

	id := hdr.Get(watermillUUID)

	md := make(message.Metadata)

	for k, v := range hdr {
		if k == watermillUUID {
			continue
		}

		if len(v) == 1 {
			md.Set(k, v[0])
		} else {
			return nil, errors.Errorf("multiple values received in NATS header for %q: (%+v)", k, v)
		}
	}

	msg := message.NewMessage(id, data)
	msg.Metadata = md

	return msg, nil
}
