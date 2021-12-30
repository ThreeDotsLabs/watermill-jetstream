package jetstream

import (
	"fmt"
	"github.com/nats-io/nats.go"
)

func initStream(js nats.JetStreamContext, topic string) error {
	//TODO: cache locally, maybe leverage available streams to keep up to date
	_, err := js.StreamInfo(topic)

	if err != nil {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:        topic,
			Description: "",
			Subjects:    []string{fmt.Sprintf("%s.*", topic)},
		})

		if err != nil {
			return err
		}
	}

	return nil
}
