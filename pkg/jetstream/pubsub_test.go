package jetstream_test

import (
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"testing"
)

func TestPublishSubscribe(t *testing.T) {
	tests.TestPubSub(
		t,
		getTestFeatures(),
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}
