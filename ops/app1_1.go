package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go <pattern> <csv_line>")
		return
	}

	// Get the pattern and CSV line from the command-line arguments
	pattern := os.Args[1]
	csvLine := os.Args[2]
	// fmt.Printf("pattern: %s, csvLine: %s\n", pattern, csvLine)

	pattern = strings.ReplaceAll(pattern, "$", " ")
	// Split the CSV line into values
	// values := strings.Split(csvLine, ",")
	csv_reader := csv.NewReader(strings.NewReader(csvLine))
	values, err := csv_reader.Read()
	if err != nil {
		fmt.Println("Error reading the provided line:", err)
		return
	}

	// Check if any value matches the pattern
	matchFound := false
	for _, value := range values {
		if value == pattern {
			// fmt.Printf("value: %s matches pattern: %s\n", value, pattern)
			matchFound = true
			break
		}
	}

	// If a match is found, print the line with values joined by "__"
	if matchFound {
		fmt.Println(strings.Join(values, "__"))
	}
}
