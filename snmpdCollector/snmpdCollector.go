package main

import (
	"context"
	jsondata "encoding/json"
	"fmt"
	"github.com/apex/log"
	"github.com/apex/log/handlers/json"
	"github.com/deejross/go-snmplib"
	"github.com/ngoralski/snmptrap2mb/logger"
	kafka "github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
)

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
		logger.LogMsg("Can't listen on UDP port", "fatal")
		panic("Failed to bind UDP Port")
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

func createSnmpListener(udpSock *net.UDPConn, writer *kafka.Writer, snmp *snmplib.SNMP, alarm chan struct{}) {

	loop := 0

	packet := make([]byte, 3000)
	for {
		_, addr, snmpErr := udpSock.ReadFromUDP(packet)
		if snmpErr != nil {
			logger.LogMsg("Error on reading udp packet", "fatal")
			alarm <- struct{}{}
			return
		}
		loop++

		logger.LogMsg(fmt.Sprintf("Received trap from %s", addr.IP), "info")

		trapData, snmpErr := snmp.ParseTrap(packet)
		if snmpErr != nil {
			//log.Printf("Error processing trap: %v.", snmpErr)
			logger.LogMsg(fmt.Sprintf("Error processing trap: %v.", snmpErr), "warn")
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
			logger.LogMsg(fmt.Sprint(kafkaErr), "error")
			alarm <- struct{}{}
		} else {

			if viper.Get("log_level") == "warn" {
				logger.MyLog = log.WithFields(log.Fields{
					"hostname":      logger.GetHostname(),
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
				logger.LogMsg("trap message received", "warn")
				logger.DefaultLogLevel()
			}
			logger.LogMsg("Message produced in kafka", "info")

		}

	}

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
			logger.LogMsg(fmt.Sprintf("Can't write to %s", outputFileName), "info")
			panic(err)
		}
		log.SetHandler(json.New(outputFile))
	}

	logger.LogMsg("Starting snmpdCollector", "info")

	logger.LogMsg("read configfile config.json", "info")

	threads := int(viper.Get("threads").(float64))
	kafkaUrl := viper.Get("kafka.raw.server").(string)
	kafkaTopic := viper.Get("kafka.raw.topic").(string)
	listenIP := viper.Get("ip").(string)
	listenPort := int(viper.Get("port").(float64))

	logger.LogMsg(fmt.Sprintf("Running for %d threads", threads), "info")

	rand.Seed(0)
	target := ""
	community := ""
	version := snmplib.SNMPv2c

	logger.LogMsg(fmt.Sprintf("Listening for snmp trap on %s:%d", listenIP, listenPort), "info")

	udpSocket := myUDPServer(listenIP, listenPort)
	defer udpSocket.Close()

	snmp := snmplib.NewSNMPOnConn(target, community, version, 2*time.Second, 5, udpSocket)
	defer snmp.Close()

	// TODO
	// Manage User inside external json configuration file

	logger.LogMsg(fmt.Sprintf("Using kafka server %s", kafkaUrl), "info")
	logger.LogMsg(fmt.Sprintf("Push messages in kafka topic %s", kafkaTopic), "info")

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
