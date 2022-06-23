package logger

import (
	"fmt"
	"github.com/apex/log"
	"github.com/apex/log/handlers/json"
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

func InitLog() {
	if viper.Get("log_output").(string) == "stdout" {
		log.SetHandler(json.New(os.Stdout))
	} else {
		outputFileName := viper.Get("log_output").(string)
		outputFile, err := os.Create(outputFileName)
		if err != nil {
			LogMsg(fmt.Sprintf("Can't write to %s", outputFileName), "info")
			panic(err)
		}
		log.SetHandler(json.New(outputFile))
	}

}

func LogMsg(msg string, severity string) {
	switch strings.ToLower(severity) {
	case "fatal":
		MyLog.Fatal(msg)
		fmt.Printf("%s\n", msg)
	case "error":
		MyLog.Error(msg)
	case "debug":
		if viper.Get("log_level") == "debug" {
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
