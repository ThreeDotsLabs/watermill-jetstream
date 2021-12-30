package jetstream_test

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/AlexCuse/watermill-jetstream/pkg/jetstream"
)

func TestGobMarshaler(t *testing.T) {
	msg := message.NewMessage("1", []byte("zag"))
	msg.Metadata.Set("foo", "bar")

	marshaler := jetstream.GobMarshaler{}

	b, err := marshaler.Marshal("topic", msg)
	require.NoError(t, err)

	unmarshaledMsg, err := marshaler.Unmarshal(&nats.Msg{Data: b})
	require.NoError(t, err)

	assert.True(t, msg.Equals(unmarshaledMsg))

	unmarshaledMsg.Ack()

	select {
	case <-unmarshaledMsg.Acked():
		// ok
	default:
		t.Fatal("ack is not working")
	}
}

func TestGobMarshaler_multiple_messages_async(t *testing.T) {
	marshaler := jetstream.GobMarshaler{}

	messagesCount := 1000
	wg := sync.WaitGroup{}
	wg.Add(messagesCount)

	for i := 0; i < messagesCount; i++ {
		go func(msgNum int) {
			defer wg.Done()

			msg := message.NewMessage(fmt.Sprintf("%d", msgNum), nil)

			b, err := marshaler.Marshal("topic", msg)
			require.NoError(t, err)

			unmarshaledMsg, err := marshaler.Unmarshal(&nats.Msg{Data: b})

			require.NoError(t, err)

			assert.True(t, msg.Equals(unmarshaledMsg))
		}(i)
	}

	wg.Wait()
}
