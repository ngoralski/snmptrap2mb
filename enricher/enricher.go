package main

import (
	jsondata "encoding/json"
	"fmt"
	"github.com/apex/log"
	"github.com/apex/log/handlers/json"
	"github.com/mcuadros/go-lookup"
	"github.com/ngoralski/snmptrap2mb/logger"
	kafka "github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"sync"
)

var wg sync.WaitGroup

//var hostname, _ = os.Hostname()
//var loggy = log.WithFields(log.Fields{
//	"hostname": hostname,
//})

type SnmpData struct {
	Version     int
	TrapType    string
	OID         string
	Other       interface{}
	Community   string
	Username    string
	Address     string
	VarBinds    []string
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

func checkCondition(condition map[string]interface{}, snmpData SnmpData) (bool, map[string]string) {

	dataEvent := make(map[string]string)
	match := false
	totalMatch := 0
	validatedCondition := false

	conditionType := fmt.Sprintf("%s", condition["type"])

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

	return validatedCondition, dataEvent

}

func parseData(msg string, enriched *kafka.Writer, unknown *kafka.Writer) {
	//data, err := json.Marshal(msg)
	dataEvent := make(map[string]string)
	var match bool
	var kafkaErr error
	var snmpData SnmpData

	// Todo
	// until message was not clearly readed and analysed / pushed to another topic do not tag it as readed
	//

	_ = jsondata.Unmarshal([]byte(msg), &snmpData)

	jsonFile, err := os.Open("./data/filters/" + snmpData.OID + ".json")
	// if we os.Open returns an error then handle it
	if err != nil {
		fmt.Println(err)
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

			match, dataEvent = checkCondition(conditions[i].(map[string]interface{}), snmpData)
			if match {
				// exit on loop condition
				break
			}
		}

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
		} else {
			kafkaErr = unknown.WriteMessages(context.Background(), kafka.Message{
				Value: []byte(string(msg)),
			})
			fmt.Printf("No match on data in topic\n")
		}

		if kafkaErr != nil {
			fmt.Println(kafkaErr)
		} else {
			fmt.Println("produced")
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
			fmt.Sprintf("Can't write to %s", outputFileName)
			panic(err)
		}
		log.SetHandler(json.New(outputFile))
	}

	logger.LogMsg("Starting message enricher", "info")

	logger.LogMsg("read configfile config.json", "info")

	threads := int(viper.Get("threads").(float64))
	inputKafkaUrl := viper.Get("kafka.raw.server").(string)
	inputTopic := viper.Get("kafka.raw.topic").(string)
	inputTopicGroup := viper.Get("kafka.raw.group").(string)
	outputKafkaUrl := viper.Get("kafka.enriched.server").(string)
	outputTopic := viper.Get("kafka.enriched.topic").(string)
	unknownKafkaUrl := viper.Get("kafka.unknown.server").(string)
	unknownTopic := viper.Get("kafka.unknown.topic").(string)

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

		go parseData(string(m.Value), writer, unknownWriter)
	}

	wg.Wait()

}
