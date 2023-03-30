package util

import (
	"fmt"
)

var (
	VERSION_NUMBER = fmt.Sprintf("%.02f", 3.44)
	VERSION        = sizeLimit + " " + VERSION_NUMBER
	COMMIT         = ""
	TAG            = "2023.3.8"
)

func Version() string {
	return VERSION + " " + COMMIT
}

func Tag() string {
	return TAG
}
