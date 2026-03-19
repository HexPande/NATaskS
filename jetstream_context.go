package natasks

import (
	"context"
	"time"
)

const managementTimeout = 30 * time.Second

func managementContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), managementTimeout)
}
