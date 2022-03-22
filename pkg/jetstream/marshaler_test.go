package jetstream_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/AlexCuse/watermill-jetstream/pkg/jetstream"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type marshalerCase struct {
	name      string
	marshaler jetstream.MarshalerUnmarshaler
}

var marshalerCases = []marshalerCase{
	{"gob", &jetstream.GobMarshaler{}},
	{"json", &jetstream.JSONMarshaler{}},
	{"nats", &jetstream.NATSMarshaler{}},
}

func TestMarshalers(t *testing.T) {
	for _, tc := range marshalerCases {
		t.Run(tc.name, func(t *testing.T) {
			msg := sampleMessage()

			marshaler := jetstream.GobMarshaler{}

			b, err := marshaler.Marshal("topic", msg)
			require.NoError(t, err)

			unmarshaledMsg, err := marshaler.Unmarshal(b)
			require.NoError(t, err)

			assert.True(t, msg.Equals(unmarshaledMsg))

			unmarshaledMsg.Ack()

			select {
			case <-unmarshaledMsg.Acked():
				// ok
			default:
				t.Fatal("ack is not working")
			}
		})
	}
}

func TestMarshalers_multiple_messages_async(t *testing.T) {
	for _, tc := range marshalerCases {
		t.Run(tc.name, func(t *testing.T) {

			messagesCount := 1000
			wg := sync.WaitGroup{}
			wg.Add(messagesCount)

			for i := 0; i < messagesCount; i++ {
				go func(msgNum int) {
					defer wg.Done()

					msg := sampleMessage()

					b, err := tc.marshaler.Marshal("topic", msg)
					require.NoError(t, err)

					unmarshaledMsg, err := tc.marshaler.Unmarshal(b)

					require.NoError(t, err)

					assert.True(t, msg.Equals(unmarshaledMsg))
				}(i)
			}

			wg.Wait()
		})
	}
}

func BenchmarkMarshalers(b *testing.B) {
	for _, tc := range marshalerCases {
		b.Run(fmt.Sprintf("%s benchmark", tc.name), func(b *testing.B) {
			msg := sampleMessage()

			for i := 0; i < b.N; i++ {
				marshaled, err := tc.marshaler.Marshal("topic", msg)
				require.NoError(b, err)

				unmarshaledMsg, err := tc.marshaler.Unmarshal(marshaled)
				require.NoError(b, err)

				assert.True(b, msg.Equals(unmarshaledMsg))

				unmarshaledMsg.Ack()

				select {
				case <-unmarshaledMsg.Acked():
					// ok
				default:
					b.Fatal("ack is not working")
				}
			}
		})
	}
}

func TestNatsMarshaler_Error_Multiple_Values_In_Single_Header(t *testing.T) {
	b := nats.NewMsg("fizz")
	b.Header.Add("foo", "bar")
	b.Header.Add("foo", "baz")

	marshaler := jetstream.NATSMarshaler{}

	_, err := marshaler.Unmarshal(b)

	require.Error(t, err)
}

func sampleMessage() *message.Message {
	msg := message.NewMessage(watermill.NewUUID(), []byte("somewhat longer string can be used here to get us closer to a `normal` message"))

	// add some metadata
	for i := 0; i < 10; i++ {
		msg.Metadata.Set(uuid.NewString(), uuid.NewString())
	}

	return msg
}
