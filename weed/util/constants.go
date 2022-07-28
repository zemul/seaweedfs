package util

import (
	"fmt"
)

var (
	VERSION_NUMBER = fmt.Sprintf("%.02f", 3.18)
	VERSION        = sizeLimit + " " + VERSION_NUMBER
	COMMIT         = ""
	TAG            = "v1.0.3"
)

func Version() string {
	return VERSION + " " + COMMIT
}

func Tag() string {
	return TAG
}
