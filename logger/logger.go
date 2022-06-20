package logger

import (
	"fmt"
	"github.com/apex/log"
	"github.com/spf13/viper"
	"os"
	"strings"
)

var hostname, _ = os.Hostname()
var MyLog = log.WithFields(log.Fields{
	"hostname": hostname,
})

func GetHostname() string {
	return hostname
}

func LogMsg(msg string, severity string) {
	switch strings.ToLower(severity) {
	case "fatal":
		MyLog.Fatal(msg)
		fmt.Printf("%s\n", msg)
	case "error":
		MyLog.Error(msg)
	case "warn":
		if viper.Get("log_level") == "warn" {
			MyLog.Warn(msg)
		}
	default:
		// by default info all other undefined values match here
		MyLog.Info(msg)
	}
}

func DefaultLogLevel() {
	MyLog = log.WithFields(log.Fields{
		"hostname": hostname,
	})
}
