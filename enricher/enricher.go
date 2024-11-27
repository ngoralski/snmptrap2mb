package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/apex/log"
	_ "github.com/lib/pq"
	"github.com/ngoralski/snmptrap2mb/logger"
	"github.com/spf13/viper"
	"reflect"
	"regexp"
	"strings"
	"sync"
)

var wg sync.WaitGroup

type Filter struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Action  string `json:"action"`
	Message string `json:"message"`
}

type SnmpData struct {
	ID          int                    `json:"id,omitempty"`
	Version     int                    `json:"version"`
	TrapType    int                    `json:"trapType"`
	OID         string                 `json:"oid"`
	ReceivedAt  string                 `json:"receivedAt"`
	Other       interface{}            `json:"other"`
	Community   string                 `json:"community"`
	Username    string                 `json:"username"`
	Address     string                 `json:"address"`
	VarBinds    map[string]interface{} `json:"varBinds"`
	VarBindOIDs []string               `json:"varBindOids"`
}

// Filter Criteria Structure
type FilterCriteria struct {
	Key      string      `json:"key"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
}

func newPostgreSQLConn(pgURL string) *sql.DB {
	db, err := sql.Open("postgres", pgURL)
	if err != nil {
		logger.LogMsg("Can't connect to PostgreSQL", "fatal")
		panic(err)
	}
	return db
}

func fetchFilters(db *sql.DB) (map[int]Filter, error) {
	filters := make(map[int]Filter)

	rows, err := db.Query(`SELECT id, name, action, message FROM filters`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var filter Filter
		if err := rows.Scan(&filter.ID, &filter.Name, &filter.Action, &filter.Message); err != nil {
			return nil, err
		}
		filters[filter.ID] = filter
	}

	return filters, nil
}

func fetchFilterCriteria(db *sql.DB) (map[int][]FilterCriteria, error) {
	filterCriteria := make(map[int][]FilterCriteria)

	rows, err := db.Query(`SELECT filter_id, key, operator, value FROM filter_criteria`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var criteria FilterCriteria
		var filterID int
		if err := rows.Scan(&filterID, &criteria.Key, &criteria.Operator, &criteria.Value); err != nil {
			return nil, err
		}
		filterCriteria[filterID] = append(filterCriteria[filterID], criteria)
	}

	return filterCriteria, nil
}

func fetchPendingMessages(db *sql.DB) ([]SnmpData, error) {
	rows, err := db.Query(`SELECT id, content FROM messages WHERE status = 'pending'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []SnmpData
	for rows.Next() {
		var id int
		var contentJSON string
		if err := rows.Scan(&id, &contentJSON); err != nil {
			return nil, err
		}
		var snmpData SnmpData
		if err := json.Unmarshal([]byte(contentJSON), &snmpData); err != nil {
			return nil, err
		}
		snmpData.ID = id
		messages = append(messages, snmpData)
	}
	return messages, nil
}

func getFieldValue(msg interface{}, key string) (interface{}, bool) {
	msgValue := reflect.ValueOf(msg)

	if msgValue.Kind() != reflect.Struct {
		return nil, false
	}

	fieldValue := msgValue.FieldByName(key)
	if !fieldValue.IsValid() {
		return nil, false
	}

	return fieldValue.Interface(), true
}

// parsePattern parses a pattern in the form of 's/regex/replacement/' and returns the regex and replacement parts as a slice
func parsePattern(patternStr string) []string {
	if len(patternStr) < 3 || patternStr[0] != 's' || patternStr[1] != '/' || patternStr[len(patternStr)-1] != '/' {
		return nil
	}

	parts := strings.SplitN(patternStr[2:len(patternStr)-1], "/", 2)
	if len(parts) != 2 {
		return nil
	}

	return parts
}

// Replace placeholders within a nested map structure
func replacePlaceholders(data map[string]interface{}, replacements map[string]string) {
	for key, value := range data {
		switch v := value.(type) {
		case string:
			for placeholder, replacement := range replacements {
				// Regex to match placeholders with or without the regex part
				// Uses non-capturing groups for placeholders with optional regex after '||'
				re := regexp.MustCompile(fmt.Sprintf(`%s(?:\|\|(.+)\|\|)?`, regexp.QuoteMeta(placeholder)))
				dynRegexp := re.FindStringSubmatch(v)
				fmt.Printf("Regex: %s, Dyn Regex: %s\n", re.String(), dynRegexp)
				//matches3 := re.FindAllStringSubmatch(v, -1)
				//fmt.Printf("Matches3: %+v\n", matches3)

				if len(dynRegexp) > 0 {
					//regexPattern := dynRegexp[1]

					parts := parsePattern(dynRegexp[1])
					if len(parts) != 2 {
						fmt.Println("Invalid pattern format")
						return
					}
					regexPattern := parts[0]
					replacementTemplate := parts[1]

					re := regexp.MustCompile(regexPattern)
					v = re.ReplaceAllStringFunc(replacement, func(m string) string {
						// Find the submatches (capture groups)
						submatches := re.FindStringSubmatch(m)
						if len(submatches) == 0 {
							return m
						}

						// Replace the $1, $2, ... in the replacement template with the matched capture groups
						replacement := replacementTemplate
						for i := 1; i < len(submatches); i++ {
							placeholder := fmt.Sprintf("$%d", i)
							replacement = strings.ReplaceAll(replacement, placeholder, submatches[i])
						}
						return replacement
					})

				}

				fmt.Printf("Replacing placeholder '%s' with '%s'\n", placeholder, replacement)
				data[key] = v

			}
			//data[key] = v
		case map[string]interface{}:
			replacePlaceholders(v, replacements) // Recursive call for nested map
		case []interface{}:
			for i, item := range v {
				if subMap, ok := item.(map[string]interface{}); ok {
					replacePlaceholders(subMap, replacements)
					v[i] = subMap
				}
			}
		}
	}
}

func applyFilters(msg SnmpData, filters map[int]Filter, criteria map[int][]FilterCriteria) (int, map[string]interface{}) {
	// Helper function to add fields to replacements map
	addToReplacements := func(replacements map[string]string, prefix string, v reflect.Value) {
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			fieldValue := v.Field(i)

			if !fieldValue.CanInterface() {
				continue
			}

			placeholder := fmt.Sprintf("##%s%s##", prefix, field.Name)
			replacements[placeholder] = fmt.Sprintf("%v", fieldValue.Interface())
		}
	}

	for filterID, filter := range filters {
		match := true
		for _, crit := range criteria[filterID] {
			// Determine the value to be checked
			value, exists := msg.VarBinds[crit.Key]
			if !exists {
				value, exists = getFieldValue(msg, crit.Key)
				if !exists {
					fmt.Printf("Filter %d: Key %s not found in message\n", filterID, crit.Key)
					match = false
					break
				}
			}

			// Log criteria evaluation for debugging
			fmt.Printf("Evaluating criteria: %+v against value: %+v\n", crit, value)

			if value == nil || crit.Value == nil {
				match = false
				break
			}

			// Compare values based on the operator
			switch crit.Operator {
			case "eq":
				if fmt.Sprintf("%v", value) != fmt.Sprintf("%v", crit.Value) {
					match = false
				}
			case "gt":
				valueFloat, ok1 := value.(float64)
				critValueFloat, ok2 := crit.Value.(float64)
				if ok1 && ok2 {
					if valueFloat <= critValueFloat {
						match = false
					}
				} else {
					match = false
				}
			case "lt":
				valueFloat, ok1 := value.(float64)
				critValueFloat, ok2 := crit.Value.(float64)
				if ok1 && ok2 {
					if valueFloat >= critValueFloat {
						match = false
					}
				} else {
					match = false
				}
			case "like":
				valueStr, ok1 := value.(string)
				critValueStr, ok2 := crit.Value.(string)
				if ok1 && ok2 {
					if !strings.Contains(valueStr, critValueStr) {
						match = false
					}
				} else {
					match = false
				}
			case "regexp":
				valueStr, ok1 := value.(string)
				critValueStr, ok2 := crit.Value.(string)
				if ok1 && ok2 {
					re, err := regexp.Compile(critValueStr)
					if err != nil || !re.MatchString(valueStr) {
						match = false
					}
				} else {
					match = false
				}
			default:
				log.Warnf("Unknown operator: %s", crit.Operator)
				match = false
			}

			if !match {
				break
			}
		}

		if match {
			fmt.Printf("Filter matched: %+v\n", filter)
			resultJSON := make(map[string]interface{})
			if err := json.Unmarshal([]byte(filter.Message), &resultJSON); err != nil {
				log.Errorf("Failed to parse filter message: %v", err)
				return -1, nil
			}

			// Initialize the replacements map.
			replacements := make(map[string]string)

			// Add all fields from msg to replacements.
			msgValue := reflect.ValueOf(msg)
			if msgValue.Kind() == reflect.Struct {
				addToReplacements(replacements, "", msgValue)
			}

			// Add all fields from msg.VarBinds to replacements.
			for varName, varValue := range msg.VarBinds {
				placeholder := fmt.Sprintf("##%s##", varName)
				replacements[placeholder] = fmt.Sprintf("%v", varValue)
			}

			// Replace placeholders in the JSON result.
			replacePlaceholders(resultJSON, replacements)
			return filterID, resultJSON
		}
	}

	return -1, nil
}

func processSNMPMessages(db *sql.DB) {
	filters, err := fetchFilters(db)
	if err != nil {
		logger.LogMsg(fmt.Sprintf("Failed to fetch filters: %v", err), "fatal")
		return
	}

	filterCriteria, err := fetchFilterCriteria(db)
	if err != nil {
		logger.LogMsg(fmt.Sprintf("Failed to fetch filter criteria: %v", err), "fatal")
		return
	}

	snmpMessages, err := fetchPendingMessages(db)
	if err != nil {
		logger.LogMsg(fmt.Sprintf("Failed to fetch messages: %v", err), "fatal")
		return
	}

	for _, msg := range snmpMessages {
		filterID, messageJson := applyFilters(msg, filters, filterCriteria)
		if filterID != -1 {
			fmt.Printf("Processing message ID: %d with filter ID: %d, Result JSON: %+v\n", msg.ID, filterID, messageJson)
			jsonData, err := json.Marshal(messageJson)
			if err != nil {
				logger.LogMsg(fmt.Sprintf("failed to marshal message JSON: %v", err), "fatal")
			}
			_, err = db.Exec(`UPDATE messages SET status = 'processed', filter_applied = $2, message = $3::jsonb, updated_at = CURRENT_TIMESTAMP WHERE id = $1`, msg.ID, filterID, jsonData)
			if err != nil {
				logger.LogMsg(fmt.Sprintf("Failed to update message status: %v", err), "fatal")
			}
		} else {
			_, err = db.Exec(`UPDATE messages SET status = 'discarded', updated_at = CURRENT_TIMESTAMP WHERE id = $1`, msg.ID)
			if err != nil {
				logger.LogMsg(fmt.Sprintf("Failed to update message status: %v", err), "fatal")
			}
			logger.LogMsg(fmt.Sprintf("No matching filter for message ID: %d", msg.ID), "info")
		}
	}
}

func main() {
	viper.AddConfigPath("./")
	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.ReadInConfig()
	logger.InitLog()

	logger.LogMsg("Starting SNMP trap processor", "info")
	logger.LogMsg("Reading config file config.json", "info")

	pgURL := viper.Get("postgres.url").(string)

	db := newPostgreSQLConn(pgURL)
	defer db.Close()

	err := db.Ping()
	if err != nil {
		panic(err)
	}

	// Run a single processing loop for debug purposes
	processSNMPMessages(db)

	// Multi-threaded processing loop for future use
	/*
	   threads := int(viper.Get("threads").(float64))

	   for i := 0; i < threads; i++ {
	       wg.Add(1)
	       go func() {
	           defer wg.Done()
	           for {
	               processSNMPMessages(db)
	               time.Sleep(5 * time.Second) // Adjust the interval as needed
	           }
	       }()
	   }

	   wg.Wait()
	*/
}
