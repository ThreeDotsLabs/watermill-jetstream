package jetstream_test

import (
	"github.com/nats-io/nats.go"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/AlexCuse/watermill-jetstream/pkg/jetstream"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/stretchr/testify/require"
)

func getTestFeatures() tests.Features {
	return tests.Features{
		ConsumerGroups:                      true,
		ExactlyOnceDelivery:                 false,
		GuaranteedOrder:                     true,
		GuaranteedOrderWithSingleSubscriber: true,
		Persistent:                          true,
		RequireSingleInstance:               false,
		NewSubscriberReceivesOldMessages:    true,
	}
}

func newPubSub(t *testing.T, clientID string, queueName string) (message.Publisher, message.Subscriber) {
	trace := os.Getenv("WATERMILL_TEST_NATS_TRACE")

	logger := watermill.NewStdLogger(true, strings.ToLower(trace) == "trace")

	natsURL := os.Getenv("WATERMILL_TEST_NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	options := []nats.Option{
		nats.RetryOnFailedConnect(true),
		nats.Timeout(30 * time.Second),
		nats.ReconnectWait(1 * time.Second),
	}

	subscriberCount := 1

	if queueName != "" {
		subscriberCount = 2
	}

	subscribeOptions := []nats.SubOpt{
		nats.DeliverAll(),
		nats.AckExplicit(),
		nats.MaxAckPending(subscriberCount),
	}

	c, err := nats.Connect(natsURL, options...)
	require.NoError(t, err)

	defer c.Close()

	jetstreamOptions := make([]nats.JSOpt, 0)

	_, err = c.JetStream()
	require.NoError(t, err)

	pub, err := jetstream.NewPublisher(jetstream.PublisherConfig{
		URL:              natsURL,
		Marshaler:        jetstream.GobMarshaler{},
		NatsOptions:      options,
		JetstreamOptions: jetstreamOptions,
		AutoProvision:    true,
	}, logger)
	require.NoError(t, err)

	sub, err := jetstream.NewSubscriber(jetstream.SubscriberConfig{
		URL:              natsURL,
		ClientID:         clientID,
		QueueGroup:       queueName,
		DurableName:      queueName,
		SubscribersCount: subscriberCount, //multiple only works if a queue group specified
		AckWaitTimeout:   30 * time.Second,
		Unmarshaler:      jetstream.GobMarshaler{},
		NatsOptions:      options,
		SubscribeOptions: subscribeOptions,
		JetstreamOptions: jetstreamOptions,
		CloseTimeout:     30 * time.Second,
		AutoProvision:    false, // tests use SubscribeInitialize
	}, logger)
	require.NoError(t, err)

	return pub, sub
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return newPubSub(t, watermill.NewUUID(), "")
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(t, watermill.NewUUID(), consumerGroup)
}

func TestPublishSubscribe(t *testing.T) {
	tests.TestPubSub(
		t,
		getTestFeatures(),
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}
