package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

func Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("No words to count")
		return
	}
	filename := os.Args[1]
	var counts map[string]int
	file, err := os.OpenFile("/home/gera3/g31/mp4/"+filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		fmt.Printf("Error opening file: %s\n", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lastLine string
	for scanner.Scan() {
		lastLine = scanner.Text()
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file: %s\n", err)
		return
	}

	if lastLine != "" {
		if err := Unmarshal([]byte(lastLine), &counts); err != nil {
			counts = make(map[string]int)
		}
	} else {
		counts = make(map[string]int)
	}

	word := os.Args[2]
	counts[word]++

	file.Truncate(0)
	file.Seek(0, 0)
	data, err := Marshal(counts)
	if err != nil {
		fmt.Printf("Error marshalling counts: %s\n", err)
	}

	if _, err := file.Write(data); err != nil {
		fmt.Printf("Error writing counts: %s\n", err)
	}

	fmt.Println("Updated counts:", counts)
}
