package wmpb

import (
	"github.com/ThreeDotsLabs/watermill-jetstream/pkg/jetstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

type NATSMarshaler struct{}

func (*NATSMarshaler) Marshal(topic string, msg *message.Message) (*nats.Msg, error) {
	pbMsg := &Message{
		Uuid:     msg.UUID,
		Metadata: msg.Metadata,
		Payload:  msg.Payload,
	}

	data, err := proto.Marshal(pbMsg)

	if err != nil {
		return nil, err
	}

	natsMsg := nats.NewMsg(jetstream.PublishSubject(topic, msg.UUID))
	natsMsg.Data = data

	return natsMsg, nil
}

func (*NATSMarshaler) Unmarshal(msg *nats.Msg) (*message.Message, error) {
	pbMsg := &Message{}

	err := proto.Unmarshal(msg.Data, pbMsg)

	if err != nil {
		return nil, err
	}

	wmMsg := message.NewMessage(pbMsg.GetUuid(), pbMsg.GetPayload())
	wmMsg.Metadata = pbMsg.GetMetadata()

	return wmMsg, nil
}
