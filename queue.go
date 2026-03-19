package natasks

import (
	"fmt"
	"strconv"
	"strings"
)

const internalScheduleToken = "__natasks_schedule"

func normalizeQueue(queue string) string {
	trimmed := strings.TrimSpace(queue)
	if trimmed == "" {
		return "default"
	}

	return trimmed
}

func queueSubject(prefix, queue string) string {
	return prefix + "." + normalizeQueue(queue)
}

func scheduleSubject(prefix string) string {
	return prefix + "." + internalScheduleToken
}

func streamSubject(prefix string) string {
	return prefix + ".*"
}

func streamSubjects(prefix string) []string {
	return []string{streamSubject(prefix)}
}

func dlqQueue(queue, suffix string) string {
	return normalizeQueue(queue) + suffix
}

func validateQueueName(queue string) error {
	if normalizeQueue(queue) == internalScheduleToken {
		return fmt.Errorf("natasks: queue %q is reserved for internal scheduling", internalScheduleToken)
	}

	return nil
}

func attemptsHeaderValue(deliveries uint64) string {
	return strconv.FormatUint(deliveries, 10)
}
