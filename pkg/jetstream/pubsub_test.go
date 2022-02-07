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
	containerName := "watermill-jetstream_nats_1" //default on linux
	if cn, found := os.LookupEnv("WATERMILL_TEST_NATS_CONTAINERNAME"); found {
		containerName = cn
	}

	return tests.Features{
		ConsumerGroups:                      true,
		ExactlyOnceDelivery:                 false,
		GuaranteedOrder:                     true,
		GuaranteedOrderWithSingleSubscriber: true,
		Persistent:                          true,
		RestartServiceCommand:               []string{"docker", "restart", containerName},
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
		nats.ReconnectWait(time.Second),
	}

	subscribeOptions := []nats.SubOpt{
		nats.DeliverAll(),
		nats.AckExplicit(),
	}

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
		ClientID:         clientID,
		QueueGroup:       queueName,
		DurableName:      queueName,
		SubscribersCount: 1, //multiple only works if a queue group specified
		AckWaitTimeout:   time.Second,
		Unmarshaler:      jetstream.GobMarshaler{},
		NatsOptions:      options,
		SubscribeOptions: subscribeOptions,
		CloseTimeout:     10 * time.Second,
	}, logger)
	require.NoError(t, err)

	return pub, sub
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return newPubSub(t, watermill.NewUUID(), "")
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(t, consumerGroup, consumerGroup)
}

func TestPublishSubscribe(t *testing.T) {
	tests.TestPubSub(
		t,
		getTestFeatures(),
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}
