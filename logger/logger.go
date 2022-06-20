package logger

import (
	"fmt"
	"github.com/apex/log"
	"github.com/spf13/viper"
	"os"
	"strings"
)

var hostname, _ = os.Hostname()
var myLog = log.WithFields(log.Fields{
	"hostname": hostname,
})

func logger(msg string, severity string) {
	switch strings.ToLower(severity) {
	case "fatal":
		myLog.Fatal(msg)
		fmt.Printf("%s\n", msg)
	case "error":
		myLog.Error(msg)
	case "warn":
		if viper.Get("log_level") == "warn" {
			myLog.Warn(msg)
		}
	default:
		// by default info all other undefined values match here
		myLog.Info(msg)
	}
}

func defaultLogLevel() {
	myLog = log.WithFields(log.Fields{
		"hostname": hostname,
	})
}
