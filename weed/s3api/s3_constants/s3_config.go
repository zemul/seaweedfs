package s3_constants

import (
	"strings"
)

var (
	CircuitBreakerConfigDir  = "/etc/s3"
	CircuitBreakerConfigFile = "circuit_breaker.json"
	AllowedActions           = []string{ACTION_READ, ACTION_WRITE, ACTION_LIST, ACTION_TAGGING, ACTION_ADMIN}
	LimitTypeCount           = "Count"
	LimitTypeBytes           = "MB"
	Separator                = ":"
)

func Concat(elements ...string) string {
	return strings.Join(elements, Separator)
}
