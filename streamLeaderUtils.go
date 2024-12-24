package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

var (
	currentTask     int
	currentDestFile string
	currentPattern  string
)

func processJob(op1_exe string, op2_exe string, hydfs_src_file string, hydfs_dest_file string, num_tasks int, pattern string) {
	fmt.Println("Processing job")
	fmt.Println(time.Now().Format("2006-01-02 15:04:05.000"))

	streamGroup = []string{}
	for i := 0; i < num_tasks; i++ {
		if listOfHashes[i%len(listOfHashes)].Alive {
			streamGroup = append(streamGroup, listOfHashes[i%len(listOfHashes)].Name)
		}
	}

	taskId := rand.Intn(1000000)
	currentTask = taskId
	currentDestFile = hydfs_dest_file
	currentPattern = pattern

	filePath := "./local/" + hydfs_src_file

	partitionFile(filePath, streamGroup, taskId)

	// start stage 0 on all nodes
	for i := 0; i < num_tasks; i++ {
		nodeToSend := streamGroup[i%len(streamGroup)]
		go startStagesAcrossNodes(nodeToSend, op1_exe, op2_exe, taskId, hydfs_dest_file, pattern)
	}
}

func startStagesAcrossNodes(node string, op1_exe string, op2_exe string, taskId int, destFilename string, pattern string) {
	fmt.Printf("Starting stages on node %s\n", node)
	var nodeNumber int
	if strings.Contains(node, "$") {
		nodeNumber = getNodeNumber(strings.Split(node, "$")[1])
		node = strings.Split(node, "$")[0]
		//mergeCountsLog(nodeNumber, getNodeNumber(node), taskId)

	} else {
		nodeNumber = getNodeNumber(node)
	}
	client, err := rpc.Dial("tcp", makeAddrPort(node, 8001))
	if err != nil {
		fmt.Printf("Error dialing to node %s: %s\n", node, err)
		return
	}
	defer client.Close()
	args := StreamTask{
		InputStream:    "task1_" + fmt.Sprintf("%d", nodeNumber) + "_" + fmt.Sprintf("%d", taskId),
		NodeNumber:     nodeNumber,
		StreamGroup:    streamGroup,
		TransformFunc1: op1_exe,
		TransformFunc2: op2_exe,
		OutputStream:   destFilename,
		TaskId:         taskId,
		Pattern:        pattern,
	}
	var reply TaskReply
	err = client.Call("StreamService.AssignTask", args, &reply)
	if err != nil {
		fmt.Printf("Error calling AssignTask on node %s: %s\n", node, err)
		return
	}
}

func partitionFile(filePath string, streamGroup []string, taskId int) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file: %s\n", err)
		return
	}
	defer file.Close()
	// read file line by line
	scanner := bufio.NewScanner(file)
	lineNumber := 0
	numTasks := len(streamGroup)
	for i := 0; i < numTasks; i++ {
		os.Remove("./local/tmpfile" + fmt.Sprintf("%d", i) + ".txt")
	}
	for scanner.Scan() {
		line := scanner.Text()
		lineNumber++
		streamGroupIdx := lineNumber % numTasks
		line = filePath + ":" + fmt.Sprintf("%d", lineNumber) + "__" + line
		appendFileOnLocal(line, "./local/tmpfile"+fmt.Sprintf("%d", streamGroupIdx)+".txt")
	}
	log.Printf("done partitioning file\n")
	for i := 0; i < numTasks; i++ {
		nodeToSend := streamGroup[i]
		trg_filename := "./hydfs/" + fmt.Sprintf("task1_%d_%d", getNodeNumber(nodeToSend), taskId)
		err := writeInAppendModeOnServer("./local/tmpfile"+fmt.Sprintf("%d", i)+".txt", trg_filename, nodeToSend, 1)
		if err != nil {
			fmt.Printf("Error writing to node %s: %s\n", nodeToSend, err)
		}
		// replicate
		currentRingIdx = getRingIndex(nodeToSend)
		nextAliveSuccessor := findAliveSuccessorOfNode(currentRingIdx)
		secondAliveSuccessor := findAliveSuccessorOfNode(nextAliveSuccessor)
		firstReplica := listOfHashes[nextAliveSuccessor].Name
		secondReplica := listOfHashes[secondAliveSuccessor].Name
		writeInAppendModeOnServer("./local/tmpfile"+fmt.Sprintf("%d", i)+".txt", trg_filename, firstReplica, 2)
		writeInAppendModeOnServer("./local/tmpfile"+fmt.Sprintf("%d", i)+".txt", trg_filename, secondReplica, 2)
		// os.Remove("tmpfile" + fmt.Sprintf("%d", i) + ".txt")
	}
	log.Printf("done sending file\n")
	// sleep for two seconds
	time.Sleep(2 * time.Second)
}

func getNodeNumber(nodeName string) int {
	idx := strings.Index(nodeName, "fa24")
	if idx == -1 {
		return -1
	}
	result, err := strconv.Atoi(nodeName[idx+13 : idx+15])
	if err != nil {
		return -1
	}
	return result
}

func writeLineOnLocal(line string, filename string) {
	// open file in write mode only
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Write to the file
	_, err = file.WriteString(line + "\n")
	if err != nil {
		log.Fatal(err)
	}
}

func appendFileOnLocal(line string, filename string) {
	// Open file in append mode
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Write to the file
	_, err = file.WriteString(line + "\n")
	if err != nil {
		log.Fatal(err)
	}
}

func getLineCount(filename string) int {
	a := exec.Command("wc", "-l", filename)
	output, err := a.Output()
	if err != nil {
		log.Fatal(err)
	}
	lineCount, err := strconv.Atoi(strings.Fields(string(output))[0])
	if err != nil {
		log.Fatal(err)
	}
	return lineCount
}

func (s *StreamService) PrintCounts(fileContent string, reply *OperationReply) error {
	fmt.Println(fileContent)
	return nil
}
