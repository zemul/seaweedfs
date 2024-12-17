package util

import (
	"fmt"
)

const HttpStatusCancelled = 499

var (
	VERSION_NUMBER = fmt.Sprintf("%.02f", 3.68)
	VERSION        = sizeLimit + " " + VERSION_NUMBER
	COMMIT         = ""
	TAG            = "2024.12.17"
)

func Version() string {
	return VERSION + " " + COMMIT
}

func Tag() string {
	return TAG
}
