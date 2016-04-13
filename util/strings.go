package util

import (
	"os"
	"regexp"

	"github.com/gocontrib/log"
)

var reEnv = regexp.MustCompile("\\$[\\w\\d_]+")

// ReplaceEnv replaces environemt variables prefixed with '$' in given string.
func ReplaceEnv(s string) string {
	return reEnv.ReplaceAllStringFunc(s, func(t string) string {
		name := t[1:]
		v := os.Getenv(name)
		if len(v) == 0 {
			log.Error("$%s is undefined", name)
		}
		return v
	})
}
