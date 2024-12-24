package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	// Check if the command-line argument is provided
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go <filename> <csv_line>")
		return
	}

	// Get the input line from the command-line argument
	filename := os.Args[1]
	input := os.Args[2]

	// Split the input using "__" as the separator
	inputlines := strings.Split(input, "\n")
	result := ""

	for line := range inputlines {
		// Split the CSV line into values
		values := strings.Split(inputlines[line], "__")

		result += fmt.Sprintf("%s,%s\n", values[2], values[3])
	}

	file, err := os.OpenFile("./hydfs/"+filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("Error opening file: %s\n", err)
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// Write the result to the file
	_, err = writer.WriteString(result)
	if err != nil {
		fmt.Printf("Error writing to file: %s\n", err)
		return
	}
}
