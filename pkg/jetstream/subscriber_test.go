package jetstream

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSubscriberSubscriptionConfig_Validate(t *testing.T) {
	tests := []struct {
		name             string
		unmarshaler      Unmarshaler
		queueGroup       string
		subscribersCount int
		wantErr          bool
	}{
		{name: "OK - 1 Subscriber", unmarshaler: &GobMarshaler{}, subscribersCount: 1, wantErr: false},
		{name: "OK - Multi Subscriber + Queue Group", unmarshaler: &GobMarshaler{}, subscribersCount: 3, queueGroup: "not empty", wantErr: false},
		{name: "Invalid - Multi Subscriber no QueueGroup", unmarshaler: &GobMarshaler{}, subscribersCount: 3, wantErr: true},
		{name: "Invalid - No Unmarshaler", unmarshaler: nil, subscribersCount: 3, queueGroup: "not empty", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &SubscriberSubscriptionConfig{
				Unmarshaler:      tt.unmarshaler,
				QueueGroup:       tt.queueGroup,
				SubscribersCount: tt.subscribersCount,
			}

			if tt.wantErr {
				require.Error(t, c.Validate())
			} else {
				require.NoError(t, c.Validate())
			}
		})
	}
}
