package natasks

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueueHelpers(t *testing.T) {
	require.Equal(t, "default", normalizeQueue(" "))
	require.Equal(t, "app.tasks.emails", queueSubject("app.tasks", "emails"))
	require.Equal(t, "app.tasks.__natasks_schedule", scheduleSubject("app.tasks"))
	require.Equal(t, "app.tasks.*", streamSubject("app.tasks"))
	require.Equal(t, []string{"app.tasks.*"}, streamSubjects("app.tasks"))
	require.Equal(t, "emails-dlq", dlqQueue("emails", "-dlq"))
	require.Equal(t, "3", attemptsHeaderValue(3))
}

func TestValidateQueueName(t *testing.T) {
	require.NoError(t, validateQueueName("emails"))
	require.EqualError(t, validateQueueName(internalScheduleToken), `natasks: queue "__natasks_schedule" is reserved for internal scheduling`)
}
