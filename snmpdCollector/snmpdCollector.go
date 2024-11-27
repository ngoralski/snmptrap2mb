package main

import (
	"database/sql"
	jsondata "encoding/json"
	"fmt"
	"github.com/apex/log"
	"github.com/deejross/go-snmplib"
	_ "github.com/lib/pq"
	"github.com/ngoralski/snmptrap2mb/logger"
	"github.com/spf13/viper"
	"math/rand"
	"net"
	"strings"
	"time"
)

type SnmpData struct {
	Version     int
	TrapType    int
	OID         string
	ReceivedAt  time.Time
	Other       interface{}
	Community   string
	Username    string
	Address     string
	VarBinds    map[string]interface{}
	VarBindOIDs []string
}

var counter int = 0

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

func newPostgreSQLConn(pgURL string) *sql.DB {
	db, err := sql.Open("postgres", pgURL)
	if err != nil {
		logger.LogMsg("Can't connect to PostgreSQL", "fatal")
		panic(err)
	}
	return db
}

func createSnmpListener(udpSock *net.UDPConn, db *sql.DB, snmp *snmplib.SNMP, alarm chan struct{}) {

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
			logger.LogMsg(fmt.Sprintf("Error processing trap: %v.", snmpErr), "warn")
			continue
		}

		var snmpData SnmpData
		snmpData.VarBinds = trapData.VarBinds
		snmpData.Address = trapData.Address
		snmpData.ReceivedAt = time.Now()
		snmpData.OID = fmt.Sprintf("%s", strings.Trim(strings.Replace(fmt.Sprint(trapData.OID), " ", ".", -1), "[]"))
		snmpData.VarBindOIDs = trapData.VarBindOIDs
		snmpData.Other = trapData.Other
		snmpData.Username = trapData.Username
		snmpData.Community = trapData.Community
		snmpData.Version = trapData.Version
		snmpData.TrapType = trapData.TrapType

		counter++

		logger.LogMsg(fmt.Sprintf("Counter Trap Received : %d\n", counter), "info")

		// Transform in json the snmp data
		prettyPrint, _ := jsondata.MarshalIndent(snmpData, "", "\t")

		_, err := db.Exec(`INSERT INTO messages (content) VALUES ($1)`, prettyPrint)
		if err != nil {
			logger.LogMsg(fmt.Sprintf("PostgreSQL Error: %s", err), "error")
			alarm <- struct{}{}
		} else {

			if viper.Get("log_level") == "info" {
				logger.MyLog = log.WithFields(log.Fields{
					"hostname":      logger.GetHostname(),
					"snmp_version":  snmpData.Version,
					"Received_at":   snmpData.ReceivedAt,
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
			logger.LogMsg("Message stored in PostgreSQL", "info")
		}
	}
}

func main() {
	viper.AddConfigPath("./")
	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.ReadInConfig()

	logger.InitLog()
	logger.LogMsg("Starting snmpdCollector", "info")
	logger.LogMsg("read configfile config.json", "info")

	threads := int(viper.Get("threads").(float64))
	pgURL := viper.Get("postgres.url").(string)
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

	logger.LogMsg(fmt.Sprintf("Using PostgreSQL database %s", pgURL), "info")

	// Establish PostgreSQL connection
	db := newPostgreSQLConn(pgURL)
	defer db.Close()

	alarm := make(chan struct{})
	for i := 0; i < threads; i++ {
		go createSnmpListener(udpSocket, db, snmp, alarm)
	}

	msg := <-alarm
	fmt.Println(msg)
}
