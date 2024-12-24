package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func shouldProcessBatch(localBatchCount int, tmpString string) bool {
	return localBatchCount > batchesDoneSoFar[tmpString]
}

func getLinesToDo(nodeNumber int, taskId int) map[string]int {
	linesToDoFile := fmt.Sprintf("linesToDo_%d_%d", nodeNumber, taskId)
	file, err := os.OpenFile("./hydfs/"+linesToDoFile, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil
	}
	defer file.Close()

	tmpLinesToDo := make(map[string]int)

	scanner := bufio.NewScanner(file)
	var lastLine string
	for scanner.Scan() {
		lastLine = scanner.Text()
	}

	if err := scanner.Err(); err != nil {
		return nil
	}

	err = Unmarshal([]byte(lastLine), &tmpLinesToDo)
	if err != nil {
		return nil
	}

	return tmpLinesToDo

}

func getLinesProcessed(nodeNumber int, taskId int) {
	linesProcessedFile := fmt.Sprintf("linesProcessed_%d_%d", nodeNumber, taskId)
	file, err := os.OpenFile("./hydfs/"+linesProcessedFile, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	defer file.Close()

	tmpLinesProcessed := make(map[string]int)

	scanner := bufio.NewScanner(file)
	var lastLine string
	for scanner.Scan() {
		lastLine = scanner.Text()
	}

	if err := scanner.Err(); err != nil {
		return
	}

	err = Unmarshal([]byte(lastLine), &tmpLinesProcessed)
	if err != nil {
		return
	}

	// go through keys of tmpLinesProcessed and add them to linesProcessed if they are not already there
	for key, value := range tmpLinesProcessed {
		if _, ok := linesProcessed[key]; !ok {
			linesProcessed[key] = value
		}
	}

	fmt.Printf("linesProcessed: %v\n", linesProcessed)

}

func getBatchesDoneSoFar(nodeNumber int, taskId int) {
	batchesDoneSoFarFile := fmt.Sprintf("batchesDoneSoFar_%d_%d", nodeNumber, taskId)
	file, err := os.OpenFile("./hydfs/"+batchesDoneSoFarFile, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	defer file.Close()

	tmpBatchesDoneSoFar := make(map[string]int)

	scanner := bufio.NewScanner(file)
	var lastLine string
	for scanner.Scan() {
		lastLine = scanner.Text()
	}

	if err := scanner.Err(); err != nil {
		return
	}

	err = Unmarshal([]byte(lastLine), &tmpBatchesDoneSoFar)
	if err != nil {
		return
	}

	fmt.Printf("tmpBatchesDoneSoFar: %v\n", tmpBatchesDoneSoFar)

	// go through keys of batchesDoneSoFar and add them to batchesDoneSoFar if they are not already there
	for key, value := range tmpBatchesDoneSoFar {
		if _, ok := batchesDoneSoFar[key]; !ok {
			fmt.Printf("adding key %s to batchesDoneSoFar\n", key)
			batchesDoneSoFar[key] = value
		}
	}
}

func dumpBatchesDoneSoFar(nodeNumber int, taskId int) {
	batchesDoneSoFarFile := fmt.Sprintf("batchesDoneSoFar_%d_%d", nodeNumber, taskId)
	file, err := os.OpenFile("./hydfs/"+batchesDoneSoFarFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("Error opening file %s: %v\n", batchesDoneSoFarFile, err)
		return
	}

	defer file.Close()

	data, err := Marshal(batchesDoneSoFar)
	if err != nil {
		fmt.Printf("Error marshalling batchesDoneSoFar: %v\n", err)
		return
	}

	_, err = file.Write(data)
	if err != nil {
		fmt.Printf("Error writing to file %s: %v\n", batchesDoneSoFarFile, err)
		return
	}

	// replicate the file to other nodes
	asyncReplicateChannel <- AsyncReplicateStruct{
		SourceFile:    "./hydfs/" + batchesDoneSoFarFile,
		DestFile:      "./hydfs/" + batchesDoneSoFarFile,
		Replica:       firstReplicaStreamGroup,
		OperationFlag: 2,
	}

	asyncReplicateChannel <- AsyncReplicateStruct{
		SourceFile:    "./hydfs/" + batchesDoneSoFarFile,
		DestFile:      "./hydfs/" + batchesDoneSoFarFile,
		Replica:       secondReplicaStreamGroup,
		OperationFlag: 2,
	}
}

func updateBachesDoneSoFar(batchId string) {
	words := strings.Split(batchId, "_")
	batchNumber, _ := strconv.Atoi(words[0])
	nodeNumberStr := words[1]
	taskIdStr := words[2]
	nodeNumber, _ := strconv.Atoi(nodeNumberStr)
	taskId, _ := strconv.Atoi(taskIdStr)
	tmpString := fmt.Sprintf("%d_%d", nodeNumber, taskId)
	if batchNumber > batchesDoneSoFar[tmpString] {
		batchesDoneSoFar[tmpString] = batchNumber
		dumpBatchesDoneSoFar(nodeNumber, taskId)
	}
}

func dumpLinesPrococessed(nodeNumber int, taskId int) {
	linesProcessedFile := fmt.Sprintf("linesProcessed_%d_%d", nodeNumber, taskId)
	file, err := os.OpenFile("./hydfs/"+linesProcessedFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("Error opening file %s: %v\n", linesProcessedFile, err)
		return
	}
	defer file.Close()

	data, err := Marshal(linesProcessed)
	if err != nil {
		fmt.Printf("Error marshalling linesProcessed: %v\n", err)
		return
	}

	_, err = file.Write(data)
	if err != nil {
		fmt.Printf("Error writing to file %s: %v\n", linesProcessedFile, err)
		return
	}

	// replicate the file to other nodes
	asyncReplicateChannel <- AsyncReplicateStruct{
		SourceFile:    "./hydfs/" + linesProcessedFile,
		DestFile:      "./hydfs/" + linesProcessedFile,
		Replica:       firstReplicaStreamGroup,
		OperationFlag: 2,
	}

	asyncReplicateChannel <- AsyncReplicateStruct{
		SourceFile:    "./hydfs/" + linesProcessedFile,
		DestFile:      "./hydfs/" + linesProcessedFile,
		Replica:       secondReplicaStreamGroup,
		OperationFlag: 2,
	}
}

func dumpLinesToDo(nodeNumber int, taskId int) {
	linesToDoFile := fmt.Sprintf("linesToDo_%d_%d", nodeNumber, taskId)
	file, err := os.OpenFile("./hydfs/"+linesToDoFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("Error opening file %s: %v\n", linesToDoFile, err)
		return
	}
	defer file.Close()

	data, err := Marshal(linesToDo)
	if err != nil {
		fmt.Printf("Error marshalling linesToDo: %v\n", err)
		return
	}

	_, err = file.Write(data)
	if err != nil {
		fmt.Printf("Error writing to file %s: %v\n", linesToDoFile, err)
		return
	}

	// replicate the file to other nodes
	asyncReplicateChannel <- AsyncReplicateStruct{
		SourceFile:    "./hydfs/" + linesToDoFile,
		DestFile:      "./hydfs/" + linesToDoFile,
		Replica:       firstReplicaStreamGroup,
		OperationFlag: 2,
	}

	asyncReplicateChannel <- AsyncReplicateStruct{
		SourceFile:    "./hydfs/" + linesToDoFile,
		DestFile:      "./hydfs/" + linesToDoFile,
		Replica:       secondReplicaStreamGroup,
		OperationFlag: 2,
	}
}

func addToLinesToDo(batchId string, lineNumber int, nodeNumber int, taskId int) {
	linesToDo[batchId] = lineNumber
	dumpLinesToDo(nodeNumber, taskId)
}

func addToLinesProcessed(batchId string, nodeNumber int, taskId int) {
	linesProcessed[batchId] = 0
	dumpLinesPrococessed(nodeNumber, taskId)
}

func removeFromLinesToDo(batchId string, nodeNumber int, taskId int) {
	delete(linesToDo, batchId)
	dumpLinesToDo(nodeNumber, taskId)
}

func savePartitionOffset(logFileName string, offset int64) bool {
	file, err := os.OpenFile("./hydfs/"+logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Error opening file %s: %v\n", logFileName, err)
		return false
	}
	defer file.Close()

	_, err = file.WriteString(fmt.Sprintf("%d\n", offset))
	if err != nil {
		fmt.Printf("Error writing new offset to file %s: %v\n", logFileName, err)
		return false
	}
	return true
}

func getPartitionOffset(logFileName string) int64 {
	file, err := os.OpenFile("./hydfs/"+logFileName, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return 0
	}
	defer file.Close()

	var offset int64
	var lastOffset int64

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		_, err := fmt.Sscanf(scanner.Text(), "%d", &offset)
		if err == nil {
			lastOffset = offset
		}
	}

	if err := scanner.Err(); err != nil {
		return 0
	}
	fmt.Printf("Last offset: %d\n", lastOffset)
	return lastOffset
}
