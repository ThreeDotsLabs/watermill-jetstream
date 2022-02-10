package jetstream

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPublisherConfig_Validate(t *testing.T) {
	tests := []struct {
		name      string
		marshaler Marshaler
		wantErr   bool
	}{
		{name: "OK - 1 Subscriber", marshaler: &GobMarshaler{}, wantErr: false},
		{name: "Invalid - No Marshaler", marshaler: nil, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &PublisherConfig{
				Marshaler: tt.marshaler,
			}

			if tt.wantErr {
				require.Error(t, c.Validate())
			} else {
				require.NoError(t, c.Validate())
			}
		})
	}
}
