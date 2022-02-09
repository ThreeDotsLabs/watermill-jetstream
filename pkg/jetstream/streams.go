package jetstream

import (
	"fmt"
	"github.com/nats-io/nats.go"
)

type SubjectCalculator func(topic string) []string

type streamTopics struct {
	js                nats.JetStreamContext
	subjectCalculator SubjectCalculator
}

func defaultSubjectCalculator(topic string) []string {
	return []string{fmt.Sprintf("%s.*", topic)}
}

func newStreamTopics(js nats.JetStreamContext, formatter SubjectCalculator) *streamTopics {
	if formatter == nil {
		formatter = defaultSubjectCalculator
	}

	return &streamTopics{
		js:                js,
		subjectCalculator: formatter,
	}
}

func (b *streamTopics) init(topic string) error {
	_, err := b.js.StreamInfo(topic)

	if err != nil {
		_, err = b.js.AddStream(&nats.StreamConfig{
			Name:        topic,
			Description: "",
			Subjects:    b.subjectCalculator(topic),
		})

		if err != nil {
			return err
		}
	}

	return nil
}
