package main

import (
	"database/sql"
	_ "database/sql"
	jsondata "encoding/json"
	"fmt"
	"github.com/apex/log"
	_ "github.com/go-sql-driver/mysql"
	"github.com/mcuadros/go-lookup"
	"github.com/ngoralski/snmptrap2mb/logger"
	kafka "github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
	"io/ioutil"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"
)

var wg sync.WaitGroup
var counter = 0

var baseDBConnections map[string]*sql.DB

type SnmpFilter struct {
	ID           int    `json:"id"`
	Name         string `json:"name"`
	AgentIP      string `json:"agent_ip"`
	Oid          string `json:"oid"`
	SpecificTrap int    `json:"specific_trap"`
	Filter       string `json:"filter"`
	Action       string `json:"action"`
	Lookup       string `json:"lookup"`
	Comment      string `json:"comment"`
}

//type TrapFilters struct {
//	Filter     string       `json:"filter"`
//	TrapFilter []TrapFilter `json:"value"`
//}

type TrapFilter struct {
	Oid   string `json:"oid"`
	Value string `json:"value"`
}

type Lookup struct {
	Dbname      string `json:"dbname"`
	Dbuser      string `json:"dbuser"`
	Dbpass      string `json:"dbpass"`
	Dbhost      string `json:"dbhost"`
	Dbport      int    `json:"dbport"`
	Table       string `json:"table"`
	KeyColumn   string `json:"key_column"`
	ValueLookup string `json:"value_lookup"`
	Columns     string `json:"columns"`
}

type SnmpData struct {
	Version     int
	TrapType    string
	OID         string
	Other       interface{}
	Community   string
	Username    string
	Address     string
	VarBinds    map[string]interface{}
	VarBindOIDs []string
}

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 1,    // 0kB
		MaxBytes: 10e5, // 1MB
	})
}

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func parseLookup(lookups map[string]interface{}, snmpData SnmpData) map[string]string {

	dataEvent := make(map[string]string)

	jsonFile, err := os.Open("./data/enrichment/" + fmt.Sprintf("%s", lookups["file"]))
	if err != nil {
		fmt.Println(err)
	}

	byteValue, _ := ioutil.ReadAll(jsonFile)
	var lookupFile map[string]interface{}
	_ = jsondata.Unmarshal([]byte(byteValue), &lookupFile)

	re := regexp.MustCompile(`\$\{([a-zA-Z_0-9]*)\}`)
	listKeys := re.FindAllStringSubmatch(fmt.Sprintf("%s", lookups["key"]), -1)

	for _, value := range listKeys {
		vRegexp := fmt.Sprintf("\\$\\{%s\\}", value[1])
		re = regexp.MustCompile(vRegexp)
		tmpInfo, err := lookup.LookupString(snmpData, value[1])
		if err == nil {
			lookups["key"] = re.ReplaceAllString(fmt.Sprintf("%s", lookups["key"]), fmt.Sprintf("%s", tmpInfo))
		}

	}

	return dataEvent

}

func checkCondition(condition map[string]interface{}, snmpData SnmpData) (bool, map[string]string, string) {

	dataEvent := make(map[string]string)
	match := false
	totalMatch := 0
	validatedCondition := false

	conditionType := fmt.Sprintf("%s", condition["type"])
	conditionAction := fmt.Sprintf("%s", condition["action"])

	switch conditionType {
	case "regexp":

		searches := condition["search"].(map[string]interface{})

		for expectedKey, expectedValue := range searches {

			var key string

			switch expectedKey {
			case "specific-trap":
				key = "Other"
			default:
				key = fmt.Sprintf("%s", expectedKey)
			}
			//key := fmt.Sprintf("%s", expectedKey)

			trapValue, _ := lookup.LookupString(snmpData, key)

			match, _ = regexp.MatchString(
				fmt.Sprintf("%s", expectedValue),
				fmt.Sprintf("%s", trapValue))

			if match {
				//dataEvent[]
				totalMatch++
			}
		}

		if totalMatch == len(searches) {
			validatedCondition = true

			newValues := condition["new_values"].([]interface{})
			dataEvent["UseCase"] = condition["UseCase"].(string)

			for k, v := range newValues[0].(map[string]interface{}) {
				// Check if new value is a static definition or dynamic
				matchDyn, _ := regexp.MatchString("^$", fmt.Sprintf("%s", v))
				if matchDyn {
					// If value is $6 we need the 6th record in varbind
					fmt.Printf("Need to make a enrichment on a dyn var")
				} else {
					switch v.(type) {
					case float64:
						dataEvent[k] = fmt.Sprintf("%v", v)
					default:
						dataEvent[k] = fmt.Sprintf("%s", v)
					}

				}
			}
		}

	default:
		fmt.Println("Unknown type of condition")

	}

	return validatedCondition, dataEvent, conditionAction

}

func enrichAction(data map[string]interface{}, lookupData string, snmpData SnmpData) map[string]interface{} {
	var lookup Lookup
	//newData := make(map[string]interface{})
	//clock := time.Now()

	err := jsondata.Unmarshal([]byte(lookupData), &lookup)
	if err != nil {
		panic(err.Error())
	}
	//fmt.Printf("Data info : %s\n", data)
	//fmt.Printf("Lookup data : %s\n", lookupData)
	//fmt.Printf("Lookup info : %s\n", lookup.KeyColumn)

	clock := time.Now()
	dbEnrichSource := fmt.Sprintf(
		"%s:%s@tcp(%s:%v)/%s", lookup.Dbuser, lookup.Dbpass,
		lookup.Dbhost, lookup.Dbport, lookup.Dbname,
	)
	//dbIdentifier := fmt.Sprintf("%s-%s", lookup.Dbhost, lookup.Dbname)

	//var sqlErr error
	//var dbEnrich *sql.DB
	//
	//if _, exists := baseDBConnections[dbIdentifier]; exists {
	//	dbEnrich = baseDBConnections[dbIdentifier]
	//} else {
	//	dbEnrich, sqlErr = sql.Open("mysql", dbEnrichSource)
	//	dbEnrich.SetMaxOpenConns(10)
	//	// Set the maximum number of idle connections
	//	dbEnrich.SetMaxIdleConns(5)
	//	// Set the maximum lifetime of a connection
	//	dbEnrich.SetConnMaxLifetime(30 * time.Minute)
	//	baseDBConnections[dbIdentifier] = dbEnrich
	//}

	dbEnrich, sqlErr := sql.Open("mysql", dbEnrichSource)
	dbEnrich.SetMaxOpenConns(10)
	// Set the maximum number of idle connections
	dbEnrich.SetMaxIdleConns(5)
	// Set the maximum lifetime of a connection
	dbEnrich.SetConnMaxLifetime(30 * time.Minute)
	fmt.Printf("P2 DB Connection : %s\n", time.Now().Sub(clock))
	clock = time.Now()

	if sqlErr != nil {
		panic(sqlErr.Error())
	}
	//defer dbEnrich.Close()

	//sqlQuery := "SELECT ? FROM ? WHERE ? = '?'"
	//valueLookup := "toto"
	//lookup.ValueLookup

	for snmpOid, snmpOidData := range snmpData.VarBinds {
		//fmt.Printf("Original Value: %s\n", value)
		//fmt.Printf("OID: %s, Value: %s\n", snmpOid, snmpOidData)
		pattern := fmt.Sprintf("$OID['%s']", snmpOid)
		lookup.ValueLookup = strings.Replace(lookup.ValueLookup, pattern, snmpOidData.(string), -1)
		//fmt.Printf("New Value: %s\n\n", value)
	}
	fmt.Printf("P2 Replacement for key lookup: %s\n", time.Now().Sub(clock))
	clock = time.Now()

	queryWithParams := fmt.Sprintf("SELECT %s FROM %s WHERE %s = '%s'", lookup.Columns, lookup.Table, lookup.KeyColumn, lookup.ValueLookup)
	//fmt.Printf("Executing query: %s\n", queryWithParams)

	//fmt.Printf("SQL Query %s, %s, %s, %v\n", sqlQuery, snmpData.Address, snmpData.OID, snmpData.Other)

	//rows, err := dbEnrich.Query(sqlQuery, lookup.Columns, lookup.Table, lookup.KeyColumn, valueLookup)
	rows, queryError := dbEnrich.Query(queryWithParams)
	fmt.Printf("P2 Query lookup: %s\n", time.Now().Sub(clock))
	clock = time.Now()

	if queryError != nil {
		panic(queryError.Error())
	}
	//defer rows.Close()

	// Get column names dynamically
	enrichColumns, columnError := rows.Columns()
	if columnError != nil {
		log.Fatal(columnError.Error())
	}

	// Create a slice of interface{} to hold the values
	//enrichData := make(map[string]string)

	if rows.Next() {
		columns := make([]string, len(enrichColumns))
		columnPointers := make([]interface{}, len(enrichColumns))
		for i, _ := range columns {
			columnPointers[i] = &columns[i]
		}

		rows.Scan(columnPointers...)

		for i, colName := range enrichColumns {
			data[colName] = columns[i]
		}
	} else {
		fmt.Println("No matching filter rules found.")
		// Send it into Kakfa review queue
	}

	fmt.Printf("P2 Query result parsing: %s\n", time.Now().Sub(clock))
	clock = time.Now()

	rows.Close()
	dbEnrich.Close()

	// Check for errors from iterating over rows.
	//if err := rows.Err(); err != nil {
	//	log.Fatal(err)
	//}

	return data
}

func performAction(snmpData SnmpData, Action string) map[string]interface{} {

	//fmt.Printf("Ok perform action on \n%s \nwith %s\n", snmpData.VarBinds, Action)
	newEvent := make(map[string]interface{})
	newEvent["source"] = snmpData.Address

	var actions map[string]interface{}
	err := jsondata.Unmarshal([]byte(Action), &actions)
	if err != nil {
		panic(err.Error())
	}

	for key, value := range actions {
		//fmt.Printf("KV : %s: %v\n", key, value)
		switch v := reflect.TypeOf(value); v.Kind() {
		case reflect.String:
			// For each OID in snmpdata provided
			// Perform a search / Replace of $OID['value'] by the correct value

			for snmpOid, snmpOidData := range snmpData.VarBinds {
				//fmt.Printf("Original Value: %s\n", value)
				//fmt.Printf("OID: %s, Value: %s\n", snmpOid, snmpOidData)
				pattern := fmt.Sprintf("$OID['%s']", snmpOid)
				value = strings.Replace(value.(string), pattern, snmpOidData.(string), -1)
				//fmt.Printf("New Value: %s\n\n", value)
			}

			newEvent[key] = value.(string)
		case reflect.Int:
			newEvent[key] = value.(int64)
		case reflect.Float64:
			newEvent[key] = value.(float64)
		}

	}

	//fmt.Printf("Result to return :\n%s\n", newEvent)

	return newEvent
}

func checkFilterTrap(snmpData SnmpData, SnmpFilter string) bool {

	var trapFilter []TrapFilter
	err := jsondata.Unmarshal([]byte(SnmpFilter), &trapFilter)
	if err != nil {
		panic(err.Error())
	}

	TotalFilter := len(trapFilter)
	MatchFilter := 0

	for _, filter := range trapFilter {
		//fmt.Printf("OID: %s, Value: %s\n", filter.Oid, filter.Value)
		//fmt.Printf("List of values%v\n", snmpData.VarBinds)
		//fmt.Printf("Value to check %v\n", snmpData.VarBinds[filter.Oid])
		if value, exists := snmpData.VarBinds[filter.Oid]; exists && value == filter.Value {
			MatchFilter++
		}
	}

	if TotalFilter == MatchFilter {
		return true
	} else {
		return false
	}

}

func parseDataSql(msg string, enriched *kafka.Writer, unknown *kafka.Writer, db *sql.DB) {
	//dataEvent := make(map[string]string)
	//var match bool
	var kafkaErr error
	var snmpData SnmpData
	//var action string
	clock := time.Now()

	_ = jsondata.Unmarshal([]byte(msg), &snmpData)

	//fmt.Printf("Need to process %s\n", msg)
	//fmt.Printf("Need to process %s\n", snmpData.VarBinds)

	// First go in DB and select infos

	//cols = []interface{}

	sqlQuery := "SELECT snmpfilters.id, snmpfilters.name, agents.ip as agent_ip, snmpfilters.oid, " +
		"snmpfilters.specific_trap, snmpfilters.filter, snmpfilters.action, snmpfilters.lookup, snmpfilters.comment " +
		"FROM snmpfilters " +
		"LEFT JOIN agents ON snmpfilters.agent_id = agents.id " +
		"WHERE INET_ATON(?) BETWEEN agents.ip_start AND agents.ip_end " +
		"AND snmpfilters.oid = ? AND snmpfilters.specific_trap = ? "

	//queryWithParams := fmt.Sprintf(sqlQuery, snmpData.Address, snmpData.OID, snmpData.Other)
	//fmt.Printf("Executing query: %s\n", queryWithParams)

	//fmt.Printf("SQL Query %s, %s, %s, %v\n", sqlQuery, snmpData.Address, snmpData.OID, snmpData.Other)

	fmt.Printf("Time calculator : %s\n", time.Now().Sub(clock))
	clock = time.Now()
	rows, err := db.Query(sqlQuery, snmpData.Address, snmpData.OID, snmpData.Other)
	if err != nil {
		panic(err.Error())
	}
	//defer rows.Close()
	fmt.Printf("Get List of Filter for host : %s\n", time.Now().Sub(clock))
	clock = time.Now()

	var snmpfilter SnmpFilter

	if rows.Next() {
		err := rows.Scan(
			&snmpfilter.ID, &snmpfilter.Name, &snmpfilter.AgentIP, &snmpfilter.Oid, &snmpfilter.SpecificTrap,
			&snmpfilter.Filter, &snmpfilter.Action, &snmpfilter.Lookup, &snmpfilter.Comment,
		)
		if err != nil {
			panic(err.Error())
		}
		//fmt.Printf("FOUND Filter match : ID: %d, Name: %s, Comment: %s\n", snmpfilter.ID, snmpfilter.Name, snmpfilter.Comment)

		//fmt.Printf(" Working on filter %s\n", snmpfilter.Filter)

		filterStatus := checkFilterTrap(snmpData, snmpfilter.Filter)

		if filterStatus {
			//fmt.Printf("Trap filtered, found a match on Host, Specific Trap and sub filters\n")
			// Here before sending a status we need to perform the associated action
			//fmt.Printf("Action : %s\n", snmpfilter.Action)
			clock = time.Now()
			enrichPhase1 := performAction(snmpData, snmpfilter.Action)
			fmt.Printf("Enrich P1 : %s\n", time.Now().Sub(clock))
			clock = time.Now()

			//fmt.Printf("Enrich Phase 1 %s\n", enrichPhase1)
			dataEvent := enrichAction(enrichPhase1, snmpfilter.Lookup, snmpData)
			fmt.Printf("Enrich P2 : %s\n", time.Now().Sub(clock))
			clock = time.Now()
			//fmt.Printf("Enrich Phase 2 %s\n", enrichPhase2)

			// Send the snmpdata enriched into process kafka queue

			var targetTopic string
			//
			//for k, v := range dataEvent {
			//	dataEvent[k] = v
			//}

			jsonString, _ := jsondata.Marshal(dataEvent)
			newMsg := kafka.Message{
				Value: []byte(string(jsonString)),
			}

			if len(dataEvent) > 0 {
				kafkaErr = enriched.WriteMessages(context.Background(), newMsg)
				targetTopic = "enriched"
			} else {
				kafkaErr = unknown.WriteMessages(context.Background(), kafka.Message{
					Value: []byte(string(msg)),
				})
				targetTopic = "unknown"
			}

			if kafkaErr != nil {
				fmt.Println(kafkaErr)
			} else {
				fmt.Printf("Trap message sent to %s topic\n", targetTopic)
			}

		} else {
			fmt.Printf("Trap discarded, found a match on Host and Specific Trap but not on sub filters\n")
			// Send it into Kakfa review queue
		}

	} else {
		fmt.Println("No matching filter rules found.")
		// Send it into Kakfa review queue
	}

	rows.Close()

}

func parseData(msg string, enriched *kafka.Writer, unknown *kafka.Writer, db sql.Conn) {
	//data, err := json.Marshal(msg)
	dataEvent := make(map[string]string)
	var match bool
	var kafkaErr error
	var snmpData SnmpData
	var action string

	// Todo
	// until message was not clearly readed and analysed / pushed to another topic do not tag it as readed
	//

	_ = jsondata.Unmarshal([]byte(msg), &snmpData)

	jsonFile, err := os.Open("./data/filters/" + snmpData.OID[1:] + ".json")

	if err != nil {
		fmt.Println(err)
		logger.LogMsg(fmt.Sprintf("No filter rules found for OID trap : %s", snmpData.OID[1:]), "info")
	} else {
		fmt.Println("Successfully Opened oid.json")
		defer jsonFile.Close()

		newEvent := make(map[string]string)

		byteValue, _ := ioutil.ReadAll(jsonFile)
		var result map[string]interface{}
		_ = jsondata.Unmarshal([]byte(byteValue), &result)

		// Charging default values
		defaults := result["defaults"].(map[string]interface{})
		defaultsValues := defaults["values"].(map[string]interface{})

		// For each k/v defined in json defaults[values] assign it to newEvent
		for key, value := range defaultsValues {
			newEvent[key] = value.(string)
		}

		// enrichment for specific defaults values defined in a json file that could override previous info.
		parseLookup(defaults["enrichment"].(map[string]interface{}), snmpData)

		// Now parse conditions that allow to manage specific use case
		conditions := result["conditions"].([]interface{})

		// Warning conditions match are like firewall rules first match stop the search
		for i := range conditions {

			match, dataEvent, action = checkCondition(conditions[i].(map[string]interface{}), snmpData)
			if match {
				// exit on loop condition
				if viper.Get("log_level") == "debug" {
					debugFields := make(map[string]interface{})
					for deKey, deValue := range dataEvent {
						debugFields[deKey] = deValue
					}
					DebugFields := log.Fields(debugFields)
					logger.MyLog = log.WithFields(DebugFields)

					logger.LogMsg("Trap message match usecase", "debug")
				}
				break
			}
		}

	}
	switch action {
	case "enrich":
		var targetTopic string
		//
		for k, v := range dataEvent {
			dataEvent[k] = v
		}

		jsonString, _ := jsondata.Marshal(dataEvent)
		newMsg := kafka.Message{
			Value: []byte(string(jsonString)),
		}

		if len(dataEvent) > 0 {
			kafkaErr = enriched.WriteMessages(context.Background(), newMsg)
			targetTopic = "enriched"
		} else {
			kafkaErr = unknown.WriteMessages(context.Background(), kafka.Message{
				Value: []byte(string(msg)),
			})
			targetTopic = "garbage collector"
		}

		if kafkaErr != nil {
			fmt.Println(kafkaErr)
		} else {
			fmt.Printf("Trap message sent to %s topic\n", targetTopic)
		}

	case "discard":
		if viper.Get("log_level") == "debug" {
			debugFields := make(map[string]interface{})
			for deKey, deValue := range dataEvent {
				debugFields[deKey] = deValue
			}
			DebugFields := log.Fields(debugFields)
			logger.MyLog = log.WithFields(DebugFields)

			logger.LogMsg("Trap message was discarded", "debug")
		}
	default:
		kafkaErr = unknown.WriteMessages(context.Background(), kafka.Message{
			Value: []byte(string(msg)),
		})

		if kafkaErr != nil {
			fmt.Println(kafkaErr)
		} else {
			fmt.Printf("Trap message sent to unknown topic\n")

		}
	}

}

func main() {

	viper.AddConfigPath("./")
	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.ReadInConfig()
	logger.InitLog()

	logger.LogMsg("Starting message enricher", "info")

	logger.LogMsg("read configfile config.json", "info")

	//mux := http.NewServeMux()
	//mux.HandleFunc("/custom_debug_path/profile", pprof.Profile)
	//logorig.Fatal(http.ListenAndServe(":7777", mux))

	threads := int(viper.Get("threads").(float64))
	inputKafkaUrl := viper.Get("kafka.raw.server").(string)
	inputTopic := viper.Get("kafka.raw.topic").(string)
	inputTopicGroup := viper.Get("kafka.raw.group").(string)
	outputKafkaUrl := viper.Get("kafka.enriched.server").(string)
	outputTopic := viper.Get("kafka.enriched.topic").(string)
	unknownKafkaUrl := viper.Get("kafka.unknown.server").(string)
	unknownTopic := viper.Get("kafka.unknown.topic").(string)

	dbHost := viper.Get("database.host").(string)
	dbPort := viper.Get("database.port").(float64)
	dbUser := viper.Get("database.user").(string)
	dbPassword := viper.Get("database.password").(string)
	dbDatabase := viper.Get("database.database").(string)
	//db_table := viper.Get("database.table").(string)

	dbSource := fmt.Sprintf("%s:%s@tcp(%s:%.0f)/%s", dbUser, dbPassword, dbHost, dbPort, dbDatabase)
	db, err := sql.Open("mysql", dbSource)
	db.SetMaxOpenConns(10)
	// Set the maximum number of idle connections
	db.SetMaxIdleConns(5)
	// Set the maximum lifetime of a connection
	db.SetConnMaxLifetime(30 * time.Minute)

	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
	fmt.Println("Db connection Success!")

	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}

	wg.Add(threads)

	// Define Kafka connections
	reader := getKafkaReader(inputKafkaUrl, inputTopic, inputTopicGroup)
	defer reader.Close()

	writer := newKafkaWriter(outputKafkaUrl, outputTopic)
	defer writer.Close()

	unknownWriter := newKafkaWriter(unknownKafkaUrl, unknownTopic)
	defer unknownWriter.Close()

	fmt.Println("start consuming ... !!")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			logger.LogMsg(fmt.Sprintf("%s", err), "fatal")
		}
		counter = counter + 1

		go parseDataSql(string(m.Value), writer, unknownWriter, db)
		fmt.Printf("Counter : %f, message at topic:%v partition:%v offset:%v	%s = %s\n", counter, m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

	}

	wg.Wait()

}
