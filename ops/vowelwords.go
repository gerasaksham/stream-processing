package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("No words to count")
		return
	}
	filename := os.Args[1]
	file, err := os.OpenFile("./hydfs/"+filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		fmt.Printf("Error opening file: %s\n", err)
		return
	}
	defer file.Close()
	// fmt.Println("after reading the file")

	words := strings.Split(os.Args[2], "\n")
	vowels := "aeiou"
	result := ""
	for _, word := range words {
		if word[0] == vowels[0] || word[0] == vowels[1] || word[0] == vowels[2] || word[0] == vowels[3] || word[0] == vowels[4] {
			result += word + "\n"
		}
	}

	if _, err := file.Write([]byte(result)); err != nil {
		fmt.Printf("Error writing counts: %s\n", err)
	}

	fmt.Println("result:", result)
}
