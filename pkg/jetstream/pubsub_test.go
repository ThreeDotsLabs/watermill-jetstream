package jetstream_test

import (
	"github.com/nats-io/nats.go"
	"os"
	"testing"
	"time"

	"github.com/AlexCuse/watermill-jetstream/pkg/jetstream"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/stretchr/testify/require"
)

func newPubSub(t *testing.T, clientID string, queueName string) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(true, true)

	natsURL := os.Getenv("WATERMILL_TEST_NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	options := []nats.Option{}

	c, err := nats.Connect(natsURL, nats.Timeout(5*time.Second))

	if err != nil {
		panic(err)
	}

	_, err = c.JetStream()
	require.NoError(t, err)

	pub, err := jetstream.NewPublisher(jetstream.PublisherConfig{
		URL:         natsURL,
		Marshaler:   jetstream.GobMarshaler{},
		NatsOptions: options,
	}, logger)
	require.NoError(t, err)

	sub, err := jetstream.NewSubscriber(jetstream.SubscriberConfig{
		URL:              natsURL,
		ClientID:         clientID + "_sub",
		QueueGroup:       queueName,
		DurableName:      "durable-name",
		SubscribersCount: 10,
		AckWaitTimeout:   time.Second,
		Unmarshaler:      jetstream.GobMarshaler{},
		NatsOptions:      options,
		CloseTimeout:     time.Second,
	}, logger)
	require.NoError(t, err)

	return pub, sub
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return newPubSub(t, watermill.NewUUID(), "test-queue")
}

func createPubSubWithDurable(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(t, consumerGroup, consumerGroup)
}

func TestPublishSubscribe(t *testing.T) {
	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:                      true,
			ExactlyOnceDelivery:                 false,
			GuaranteedOrder:                     true,
			GuaranteedOrderWithSingleSubscriber: true,
			Persistent:                          true,
			RestartServiceCommand:               []string{"docker", "restart", "watermill-jetstream_nats_1"},
			RequireSingleInstance:               false,
			NewSubscriberReceivesOldMessages:    false,
		},
		createPubSub,
		createPubSubWithDurable,
	)
}
