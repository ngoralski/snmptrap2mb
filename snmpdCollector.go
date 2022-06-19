package main

import (
	"context"
	jsondata "encoding/json"
	"fmt"
	"github.com/apex/log"
	"github.com/apex/log/handlers/json"
	"github.com/deejross/go-snmplib"
	kafka "github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
)

var hostname, _ = os.Hostname()
var loggy = log.WithFields(log.Fields{
	"hostname": hostname,
})

type SnmpData struct {
	Version     int
	TrapType    int
	OID         string
	Other       interface{}
	Community   string
	Username    string
	Address     string
	VarBinds    map[string]interface{}
	VarBindOIDs []string
}

func myUDPServer(listenIPAddr string, port int) *net.UDPConn {
	addr := net.UDPAddr{
		Port: port,
		IP:   net.ParseIP(listenIPAddr),
	}
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		logger("Can't listen on UDP port", "fatal")
	}
	return conn
}

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}
func logger(msg string, severity string) {
	switch strings.ToLower(severity) {
	case "fatal":
		loggy.Fatal(msg)
		fmt.Printf(msg)
		panic("msg")
	case "error":
		loggy.Error(msg)
	case "debug":
		if viper.Get("log_level") == "debug" {
			loggy.Debug(msg)
		}
	default:
		// by default info all other undefined values match here
		loggy.Info(msg)
	}
}

func defaultLogLevel() {
	loggy = log.WithFields(log.Fields{
		"hostname": hostname,
	})
}

func createSnmpListener(udpSock *net.UDPConn, writer *kafka.Writer, snmp *snmplib.SNMP, alarm chan struct{}) {

	loop := 0

	packet := make([]byte, 3000)
	for {
		_, addr, snmpErr := udpSock.ReadFromUDP(packet)
		if snmpErr != nil {
			logger("Error on reading udp packet", "fatal")
		}
		loop++

		logger(fmt.Sprintf("Received trap from %s", addr.IP), "info")

		trapData, snmpErr := snmp.ParseTrap(packet)
		if snmpErr != nil {
			//log.Printf("Error processing trap: %v.", snmpErr)
			logger(fmt.Sprintf("Error processing trap: %v.", snmpErr), "debug")
			continue
		}

		//fmt.Printf("VB : %T\n", trapData.)

		// Push json data in SnmpData structure
		var snmpData SnmpData
		snmpData.VarBinds = trapData.VarBinds
		snmpData.Address = trapData.Address
		snmpData.OID = fmt.Sprintf("%s", strings.Trim(strings.Replace(fmt.Sprint(trapData.OID), " ", ".", -1), "[]"))
		snmpData.VarBindOIDs = trapData.VarBindOIDs
		snmpData.Other = trapData.Other
		snmpData.Username = trapData.Username
		snmpData.Community = trapData.Community
		snmpData.Version = trapData.Version
		snmpData.TrapType = trapData.TrapType

		// Transform in json the snmp data
		prettyPrint, _ := jsondata.MarshalIndent(snmpData, "", "\t")

		msg := kafka.Message{
			Value: []byte(string(prettyPrint)),
		}

		kafkaErr := writer.WriteMessages(context.Background(), msg)
		if kafkaErr != nil {
			logger(fmt.Sprint(kafkaErr), "error")
		} else {
			logger("Message produced in kafka", "info")
			loggy = log.WithFields(log.Fields{
				"hostname":      hostname,
				"snmp_version":  snmpData.Version,
				"trap_type":     snmpData.TrapType,
				"oid":           snmpData.OID,
				"specific_trap": snmpData.Other,
				"community":     snmpData.Community,
				"username":      snmpData.Username,
				"address":       snmpData.Address,
				"varbindoids":   snmpData.VarBindOIDs,
				"varbinds":      snmpData.VarBinds,
			})
		}

	}
	alarm <- struct{}{}
}

func main() {

	viper.AddConfigPath("./")
	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.ReadInConfig()

	if viper.Get("log_output").(string) == "stdout" {
		log.SetHandler(json.New(os.Stdout))
	} else {
		outputFileName := viper.Get("log_output").(string)
		outputFile, err := os.Create(outputFileName)
		if err != nil {
			fmt.Sprintf("Can't write to %s", outputFileName)
			panic(err)
		}
		log.SetHandler(json.New(outputFile))
	}

	logger("Starting snmpdCollector", "info")

	logger("read configfile config.json", "info")

	threads := int(viper.Get("threads").(float64))
	kafkaUrl := viper.Get("kafka.raw.server").(string)
	kafkaTopic := viper.Get("kafka.raw.topic").(string)
	listenIP := viper.Get("ip").(string)
	listenPort := int(viper.Get("port").(float64))

	logger(fmt.Sprintf("Running for %d threads", threads), "info")

	rand.Seed(0)
	target := ""
	community := ""
	version := snmplib.SNMPv2c

	logger(fmt.Sprintf("Listening for snmp trap on %s:%d", listenIP, listenPort), "info")

	udpSocket := myUDPServer(listenIP, listenPort)
	defer udpSocket.Close()

	snmp := snmplib.NewSNMPOnConn(target, community, version, 2*time.Second, 5, udpSocket)
	defer snmp.Close()

	// TODO
	// Manage User inside external json configuration file

	logger(fmt.Sprintf("Using kafka server %s", kafkaUrl), "info")
	logger(fmt.Sprintf("Push messages in kafka topic %s", kafkaTopic), "info")

	//Define Kafka connections
	writer := newKafkaWriter(kafkaUrl, kafkaTopic)
	defer writer.Close()

	alarm := make(chan struct{})
	for i := 0; i < threads; i++ {
		go createSnmpListener(udpSocket, writer, snmp, alarm)
	}

	msg := <-alarm
	fmt.Println(msg)

}
