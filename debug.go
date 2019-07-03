package sedi

import (
	"log"
	"encoding/json"
)

const debugMode = false

func logDebug(s string) {
	if debugMode {
		log.Println("Session manager: " + s)
	}
}

func Stringify( o interface{}) string {
	x, _ := json.MarshalIndent(o, "", "    ")
	return string(x)
}

