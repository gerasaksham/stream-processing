package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 3 { // Ensure at least 3 arguments are provided
		log.Println("Usage: <program> <pattern> <line>")
		return
	}

	pattern := os.Args[1]
	line := os.Args[2]

	// Replace $ with a space in the pattern
	pattern = strings.ReplaceAll(pattern, "$", " ")

	// Split the line into columns using comma as the delimiter
	// columns := strings.Split(line, ",")
	csv_reader := csv.NewReader(strings.NewReader(line))
	columns, err := csv_reader.Read()
	if err != nil {
		log.Println("Error reading the provided line:", err)
		return
	}

	// Ensure there are at least 8 columns to safely access the 7th and 8th columns
	if len(columns) < 8 {
		log.Println("Error: The provided line does not have enough columns.")
		return
	}

	if columns[6] == pattern {
		fmt.Println(columns[8])
	}
}
