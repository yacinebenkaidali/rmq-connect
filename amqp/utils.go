package amqp

import (
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"
)

func getRetryCountFromHeaders(headers *amqp.Table) int {
	var count int
	switch v := (*headers)[FAILED_MSG_RETRY_COUNT_HEADER].(type) {
	case int8:
		count = int(v)
	case uint8: // byte
		count = int(v)
	case int16:
		count = int(v)
	case uint16:
		count = int(v)
	case int32:
		count = int(v)
	case uint32:
		count = int(v)
	case int64:
		count = int(v)
	case float32:
		count = int(v)
	case float64:
		count = int(v)
	case string: // in case a producer sent it as text
		count, _ = strconv.Atoi(v)
	}
	return count
}
