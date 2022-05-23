package util

import (
	"fmt"
)

var (
	VERSION_NUMBER = fmt.Sprintf("%.02f", 2.89)
	VERSION        = sizeLimit + " " + VERSION_NUMBER
	COMMIT         = ""
	TAG            = "v1.0.2"
)

func Version() string {
	return VERSION + " " + COMMIT
}

func Tag() string {
	return TAG
}
