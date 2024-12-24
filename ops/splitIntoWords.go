package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		return
	}
	line := os.Args[1]
	words := strings.Split(line, ",")
	for _, word := range words {
		fmt.Println(word)
	}
}
