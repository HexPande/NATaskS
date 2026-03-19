package natasks

import (
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func TestStreamConfig(t *testing.T) {
	cfg := streamConfig(config{
		streamName:    "APP",
		subjectPrefix: "app.tasks",
	})

	require.Equal(t, "APP", cfg.Name)
	require.Equal(t, []string{"app.tasks.*"}, cfg.Subjects)
	require.Equal(t, jetstream.WorkQueuePolicy, cfg.Retention)
	require.Equal(t, jetstream.FileStorage, cfg.Storage)
	require.Equal(t, jetstream.DiscardOld, cfg.Discard)
	require.True(t, cfg.AllowMsgSchedules)
}

func TestHasStreamSubject(t *testing.T) {
	require.True(t, hasStreamSubject([]string{"app.tasks.*"}, "app.tasks"))
	require.False(t, hasStreamSubject([]string{"other.*"}, "app.tasks"))
}
