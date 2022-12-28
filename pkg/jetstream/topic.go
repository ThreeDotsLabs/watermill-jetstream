package jetstream

import (
	"github.com/nats-io/nats.go"
)

// SubjectCalculator is a function used to calculate nats subject(s) for the given topic.
type SubjectCalculator func(topic string) *Subjects

// DurableNameCalculator is a function used to calculate nats durable names for the given topic.
type DurableNameCalculator func(durableName, topic string) string

// QueueGroupCalculator is a function used to calculate nats queue group for the given topic.
type QueueGroupCalculator func(queueGroup, topic string) string

// Subjects contains nats subject detail (primary + all additional) for a given watermill topic.
type Subjects struct {
	Primary    string
	Additional []string
}

// All combines the primary and all additional subjects for use by the nats client on creation.
func (s *Subjects) All() []string {
	return append([]string{s.Primary}, s.Additional...)
}

type topicInterpreter struct {
	js                    nats.JetStreamManager
	subjectCalculator     SubjectCalculator
	durableNameCalculator DurableNameCalculator
	queueGroupCalculator  QueueGroupCalculator
}

func defaultSubjectCalculator(topic string) *Subjects {
	return &Subjects{
		Primary: topic,
	}
}

func defaultDurableNameCalculator(durableName, _ string) string {
	return durableName
}

func defaultQueueGroupCalculator(queueGroup, _ string) string {
	return queueGroup
}

func newTopicInterpreter(
	js nats.JetStreamManager,
	subjectCalculator SubjectCalculator,
	durableNameCalculator DurableNameCalculator,
	queueGroupCalculator QueueGroupCalculator,
) *topicInterpreter {
	if subjectCalculator == nil {
		subjectCalculator = defaultSubjectCalculator
	}
	if durableNameCalculator == nil {
		durableNameCalculator = defaultDurableNameCalculator
	}
	if queueGroupCalculator == nil {
		queueGroupCalculator = defaultQueueGroupCalculator
	}

	return &topicInterpreter{
		js:                    js,
		subjectCalculator:     subjectCalculator,
		durableNameCalculator: defaultDurableNameCalculator,
		queueGroupCalculator:  defaultQueueGroupCalculator,
	}
}

func (b *topicInterpreter) ensureStream(topic string) error {
	_, err := b.js.StreamInfo(topic)

	if err != nil {
		_, err = b.js.AddStream(&nats.StreamConfig{
			Name:        topic,
			Description: "",
			Subjects:    b.subjectCalculator(topic).All(),
		})

		if err != nil {
			return err
		}
	}

	return err
}
