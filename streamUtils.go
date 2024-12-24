package main

import (
	"bufio"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

var (
	counterMutex          sync.Mutex
	execOpMutex           sync.Mutex
	asyncReplicateChannel = make(chan AsyncReplicateStruct, 1000)
	batchesDoneSoFar      = make(map[string]int)
	linesProcessed        = make(map[string]int)
	linesToDo             = make(map[string]int)
	batchMutex            sync.Mutex
	linesMutex            sync.Mutex
)

type StreamService struct{}

type StreamTask struct {
	InputStream    string
	TransformFunc1 string
	TransformFunc2 string
	StreamGroup    []string
	OutputStream   string
	TaskId         int
	NodeNumber     int
	Pattern        string
}

type TaskReply struct {
	Ack bool
}

var (
	firstReplicaStreamGroup  string
	secondReplicaStreamGroup string
	currentRingIdx           int
)

func mergeCountsLog(failedNode int, CurrNode int, taskId int) {
	execOpMutex.Lock()
	defer execOpMutex.Unlock()
	failedLogFile := fmt.Sprintf("counts_%d_%d.log", failedNode, taskId)
	currLogFile := fmt.Sprintf("counts_%d_%d.log", CurrNode, taskId)
	failedFile, err := os.OpenFile("./hydfs/"+failedLogFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		fmt.Printf("Error opening file: %s\n", err)
		return
	}
	defer failedFile.Close()
	currFile, err := os.OpenFile("./hydfs/"+currLogFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		fmt.Printf("Error opening file: %s\n", err)
		return
	}
	defer currFile.Close()

	var failedCounts map[string]int
	scanner := bufio.NewScanner(failedFile)
	var lastFailedLine string
	for scanner.Scan() {
		lastFailedLine = scanner.Text()
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file: %s\n", err)
		return
	}

	if lastFailedLine != "" {
		if err := Unmarshal([]byte(lastFailedLine), &failedCounts); err != nil {
			failedCounts = make(map[string]int)
		}
	} else {
		failedCounts = make(map[string]int)
	}

	fmt.Printf("Failed counts: %v\n", failedCounts)

	var currCounts map[string]int
	scanner = bufio.NewScanner(currFile)
	var lastCurrLine string
	for scanner.Scan() {
		lastCurrLine = scanner.Text()
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file: %s\n", err)
		return
	}

	if lastCurrLine != "" {
		if err := Unmarshal([]byte(lastCurrLine), &currCounts); err != nil {
			currCounts = make(map[string]int)
		}
	} else {
		currCounts = make(map[string]int)
	}

	fmt.Printf("Current counts before: %v\n", currCounts)

	for key, value := range failedCounts {
		currCounts[key] += value
	}

	fmt.Printf("Current counts after: %v\n", currCounts)

	currFile.Truncate(0)
	currFile.Seek(0, 0)
	data, err := Marshal(currCounts)
	fmt.Printf("Merged counts: %s\n", string(data))
	if err != nil {
		fmt.Printf("Error marshalling data: %s\n", err)
		return
	}
	_, err = currFile.WriteString(string(data) + "\n")
	if err != nil {
		fmt.Printf("Error writing to file: %s\n", err)
		return
	}

}

func (s *StreamService) AssignTask(task StreamTask, reply *TaskReply) error {
	fmt.Printf("assign task called\n")
	fmt.Printf("Received task args: %v\n", task)
	currentRingIdx = getRingIndex(selfAddr)
	nextAliveSuccessor := findAliveSuccessorOfNode(currentRingIdx)
	secondAliveSuccessor := findAliveSuccessorOfNode(nextAliveSuccessor)
	firstReplicaStreamGroup = listOfHashes[nextAliveSuccessor].Name
	secondReplicaStreamGroup = listOfHashes[secondAliveSuccessor].Name
	buildBinaries(task.TransformFunc1, task.TransformFunc2)
	go asyncReplicate()
	if task.NodeNumber != getNodeNumber(selfAddr) {
		mergeCountsLog(task.NodeNumber, getNodeNumber(selfAddr), task.TaskId)
		// replicate counts log
		count_logs_file := fmt.Sprintf("counts_%d_%d.log", getNodeNumber(selfAddr), task.TaskId)
		asyncReplicateChannel <- AsyncReplicateStruct{
			SourceFile:    "./hydfs/" + count_logs_file,
			DestFile:      "./hydfs/" + count_logs_file,
			Replica:       firstReplicaStreamGroup,
			OperationFlag: 2,
		}
		asyncReplicateChannel <- AsyncReplicateStruct{
			SourceFile:    "./hydfs/" + count_logs_file,
			DestFile:      "./hydfs/" + count_logs_file,
			Replica:       secondReplicaStreamGroup,
			OperationFlag: 2,
		}
	}
	go startTask(task)
	reply.Ack = true
	return nil
}

type OperationArgs struct {
	Operation   string
	NodeNumber  int
	CmdLineArgs []string
	WordId      string
	TaskId      int
}

type OperationReply struct {
	Ack    bool
	Output string
}

func indexOf(slice []string, target string) int {
	for i, v := range slice {
		if v == target {
			return i
		}
	}
	return -1
}

type AsyncReplicateStruct struct {
	SourceFile    string
	DestFile      string
	Replica       string
	OperationFlag int
}

func asyncReplicate() {
	for {
		select {
		case asyncReplicateStruct := <-asyncReplicateChannel:
			err := writeFileOnServer(asyncReplicateStruct.SourceFile, asyncReplicateStruct.DestFile, asyncReplicateStruct.Replica, asyncReplicateStruct.OperationFlag)
			if err != nil {
				log.Printf("Error replicating file: %s\n", err)
			}
		}
	}
}

func executeOp(executable string, cmdArgs []string) string {
	execOpMutex.Lock()
	defer execOpMutex.Unlock()
	// fmt.Printf("Executing operation %s with args %v\n", executable, cmdArgs)

	cmd := exec.Command("./ops/"+executable, cmdArgs...)

	// Capture both stdout and stderr
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Error executing operation %s: %s\n", executable, err)
		return string(output)
	}

	log.Printf("Output of operation %s: %s\n", executable, output)
	return string(output)
}

func stgTwoProcessing(filename string, taskId int, lineToProcess int, operation string, nodeNumber int, batchId string) {

	// skip first lineToProcess - 1 lines
	file, err := os.Open("./hydfs/" + filename)
	if err != nil {
		log.Printf("Error opening file: %s\n", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	maxCapacity := 1024 * 1024 * 2
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	for i := 0; i < lineToProcess-1; i++ {
		scanner.Scan()
	}

	for scanner.Scan() {
		line := scanner.Text()
		words := strings.Split(line, "\t")
		log.Printf("length of words: %d\n", len(words))
		wordsSepByN := strings.Join(words, "\n")

		// exec op2
		counts_file := fmt.Sprintf("counts_%d_%d.log", getNodeNumber(selfAddr), taskId)
		// fmt.Printf("counts file: %s", counts_file)
		// fmt.Printf("wordsSepByN: %s", wordsSepByN)
		_ = executeOp(operation, []string{counts_file, wordsSepByN})

		// update linesProcessed
		addToLinesProcessed(batchId, nodeNumber, taskId)
		// update linesToDo
		removeFromLinesToDo(batchId, nodeNumber, taskId)

		ofile, err := os.OpenFile("./hydfs/"+counts_file, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			fmt.Printf("Error opening file: %s\n", err)
			return
		}
		defer ofile.Close()

		// replicate
		asyncReplicateChannel <- AsyncReplicateStruct{
			SourceFile:    "./hydfs/" + counts_file,
			DestFile:      "./hydfs/" + counts_file,
			Replica:       firstReplicaStreamGroup,
			OperationFlag: 2,
		}
		asyncReplicateChannel <- AsyncReplicateStruct{
			SourceFile:    "./hydfs/" + counts_file,
			DestFile:      "./hydfs/" + counts_file,
			Replica:       secondReplicaStreamGroup,
			OperationFlag: 2,
		}

		// read the contents of the file
		ofileScanner := bufio.NewScanner(ofile)
		var fileContent string
		for ofileScanner.Scan() {
			fileContent += ofileScanner.Text() + "\n"
		}

		if err := ofileScanner.Err(); err != nil {
			fmt.Printf("Error reading file: %s\n", err)
			return
		}

		fmt.Printf("fileContent: %s\n", fileContent)

		// send fileContent to introducer
		client, err := rpc.Dial("tcp", makeAddrPort(IntroducerAddr, 8001))
		if err != nil {
			fmt.Printf("Error dialing to introducer: %s\n", err)
			return
		}
		defer client.Close()
		client.Call("StreamService.PrintCounts", fileContent, &OperationReply{})

		break
	}
}

func (s *StreamService) StartPhaseTwo(operationArgs OperationArgs, reply *OperationReply) error {
	// write wordId__word to file
	stgTwoIpFilename := fmt.Sprintf("stg2ip_%d_%d.log", operationArgs.NodeNumber, operationArgs.TaskId)
	// append only if the wordId is not already present
	// for _, word := range operationArgs.CmdLineArgs {
	// 	appendFileOnLocal(word, "./hydfs/"+stgTwoIpFilename)
	// }

	joinedWords := strings.Join(operationArgs.CmdLineArgs, "\t")
	// batchIdStr := operationArgs.WordId
	// change batchId to int
	batchId := operationArgs.WordId

	// check if batchId present in linesProcessed
	if _, exists := linesProcessed[batchId]; !exists {
		fmt.Printf("batchId %s not processed\n", batchId)
		appendFileOnLocal(joinedWords, "./hydfs/"+stgTwoIpFilename)
		lineNumber := getLineCount("./hydfs/" + stgTwoIpFilename)
		addToLinesToDo(batchId, lineNumber, operationArgs.NodeNumber, operationArgs.TaskId)
		go stgTwoProcessing(stgTwoIpFilename, operationArgs.TaskId, lineNumber, operationArgs.Operation, operationArgs.NodeNumber, batchId)
		// replicate the file
		asyncReplicateChannel <- AsyncReplicateStruct{
			SourceFile:    "./hydfs/" + stgTwoIpFilename,
			DestFile:      "./hydfs/" + stgTwoIpFilename,
			Replica:       firstReplicaStreamGroup,
			OperationFlag: 2,
		}
		asyncReplicateChannel <- AsyncReplicateStruct{
			SourceFile:    "./hydfs/" + stgTwoIpFilename,
			DestFile:      "./hydfs/" + stgTwoIpFilename,
			Replica:       secondReplicaStreamGroup,
			OperationFlag: 2,
		}
	} else {
		fmt.Printf("batchId %s already processed\n", batchId)
	}

	reply.Ack = true
	return nil
}

func sendToTransformFunc2(task StreamTask, wordMap map[string]string, batchId string) error {
	nodeMap := make(map[int][]string)
	// tmpstring := fmt.Sprintf("%d_%d", task.NodeNumber, task.TaskId)
	for wordId, word := range wordMap {
		sgMutex.Lock()
		nodeIdxToSend := int(HashString(word)) % len(streamGroup)
		sgMutex.Unlock()
		log.Printf("sending word %s with unique id %s to node %d\n", word, wordId, nodeIdxToSend)
		log.Printf("Stream Group: %v\n", streamGroup)
		// log.Printf("sending from node %s\n", selfAddr)
		// payload := fmt.Sprintf("%s__%s", wordId, word)
		payload := fmt.Sprintf("%s", word)
		nodeMap[nodeIdxToSend] = append(nodeMap[nodeIdxToSend], payload)
	}
	// log size of each key in nodeMap
	for key, value := range nodeMap {
		log.Printf("Node %d has %d words\n", key, len(value))
	}

	// nodeNumberMap := make(map[string]int)
	// for idx, node := range streamGroup {
	// 	nodeNumberMap[node] = idx
	// }

	for idx, payload := range nodeMap {
		client, err := rpc.Dial("tcp", makeAddrPort(streamGroup[idx], 8001))
		if err != nil {
			fmt.Printf("Error dialing to node %s: %s\n", streamGroup[idx], err)
			fmt.Printf("Attempting wordId: %s\n", batchId+"_"+fmt.Sprintf("%d", idx))
			return err
		}
		var reply OperationReply
		operationArgs := OperationArgs{
			Operation:   task.TransformFunc2,
			NodeNumber:  getNodeNumber(streamGroup[idx]),
			CmdLineArgs: payload,
			TaskId:      task.TaskId,
			WordId:      batchId + "_" + fmt.Sprintf("%d", idx),
		}
		err = client.Call("StreamService.StartPhaseTwo", operationArgs, &reply)
		client.Close()
		if err == nil && reply.Ack {
		} else if err != nil {
			fmt.Printf("Error executing operation %s on node %s: %s\n", task.TransformFunc2, streamGroup[idx], err)
			return err
		}
		log.Printf("Operation %s executed successfully on node %s\n", task.TransformFunc2, streamGroup[idx])
	}
	return nil
}

func processBatch(task StreamTask, batch []string, batchId string) {
	// Apply TransformFunc1 on the batch
	wordMap := make(map[string]string)
	for _, tuple := range batch {
		tupleValue := strings.Split(tuple, "__")[1]
		cmdLineArgs := []string{tupleValue}
		lineId := strings.Split(tuple, "__")[0]
		if task.Pattern != "" {
			patternWords := strings.Split(task.Pattern, " ")
			// fmt.Printf("patternWords: %v\n", patternWords)
			newPattern := strings.Join(patternWords, "$")
			cmdLineArgs = append([]string{newPattern}, cmdLineArgs...)
		}
		// if idx == 1 {
		// 	fmt.Printf("cmdLineArgs: %v\n", cmdLineArgs)
		// }

		splitWords := executeOp(task.TransformFunc1, cmdLineArgs)
		words := strings.Split(splitWords, "\n")
		for idx, word := range words {
			if word != "" {
				wordId := fmt.Sprintf("%d_%s_%d", task.NodeNumber, lineId, idx)
				wordMap[wordId] = word
			}
		}
	}
	const maxRetries = 100

	for i := 0; i < maxRetries; i++ {
		err := sendToTransformFunc2(task, wordMap, batchId)
		if err == nil {
			break
		}
		time.Sleep(2 * time.Second)
		if i == maxRetries-1 {
			// Handle the case where all retries failed
			// sleep for two seconds
			fmt.Printf("sendToTransformFunc2 failed after 100 retries")
			return
		}
	}
	updateBachesDoneSoFar(batchId)
}

func processTuples(task StreamTask, tupleChannel chan string) {
	fmt.Printf("Started processTuples with task %v\n", task)
	batchSize := 500
	var batch []string
	tmpString := fmt.Sprintf("%d_%d", task.NodeNumber, task.TaskId)

	localBatchCount := 0
	for tuple := range tupleChannel {
		batch = append(batch, tuple)
		if len(batch) >= batchSize {
			localBatchCount++
			if shouldProcessBatch(localBatchCount, tmpString) {
				batchId := fmt.Sprintf("%d_%d_%d", localBatchCount, task.NodeNumber, task.TaskId)
				processBatch(task, batch, batchId)
			} else {
				fmt.Printf("skipping batch %d\n", localBatchCount)
			}
			batch = nil
			fmt.Printf("clearing batch\n")
		}
	}

	// Process any remaining tuples
	if len(batch) > 0 {
		log.Printf("last batch size: %d\n", len(batch))
		localBatchCount++
		if shouldProcessBatch(localBatchCount, tmpString) {
			batchId := fmt.Sprintf("%d_%d_%d", localBatchCount, task.NodeNumber, task.TaskId)
			processBatch(task, batch, batchId)
		}
	}
	log.Println("End of processTuples")
}

func startTask(task StreamTask) {
	// initialize important variables
	fmt.Printf("pattern received: %s\n", task.Pattern)

	// update linesProcessed and linesToDo and batchesDoneSoFar
	tmpLinesToDo := getLinesToDo(task.NodeNumber, task.TaskId)
	getLinesProcessed(task.NodeNumber, task.TaskId)
	getBatchesDoneSoFar(task.NodeNumber, task.TaskId)
	tmpString := fmt.Sprintf("%d_%d", task.NodeNumber, task.TaskId)
	if _, ok := batchesDoneSoFar[tmpString]; !ok {
		batchesDoneSoFar[tmpString] = 0
	}

	// print state
	fmt.Printf("linesProcessed: %v\n", linesProcessed)
	fmt.Printf("tmpLinesToDo: %v\n", tmpLinesToDo)
	fmt.Printf("linesToDo: %v\n", linesToDo)
	fmt.Printf("batchesDoneSoFar: %v\n", batchesDoneSoFar)

	// check if there are any lines to do
	if tmpLinesToDo != nil && len(tmpLinesToDo) != 0 {
		for key, value := range tmpLinesToDo {
			log.Printf("executing pending task with batchId: %s\n", key)
			stgTwoProcessing(task.InputStream, task.TaskId, value, task.TransformFunc2, task.NodeNumber, key)
		}
	}

	sgMutex.Lock()
	streamGroup = task.StreamGroup
	sgMutex.Unlock()

	tupleChannel := make(chan string, 10000)
	var wg sync.WaitGroup

	// Start the worker goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		processTuples(task, tupleChannel)
	}()

	// Read tuples and send to the channel
	inputFile, err := os.Open("./hydfs/" + task.InputStream)
	if err != nil {
		log.Printf("Error opening input stream: %v\n", err)
		return
	}
	defer inputFile.Close()

	scanner := bufio.NewScanner(inputFile)
	fmt.Printf("Reading input stream now\n")
	lineNumber := 0
	for scanner.Scan() {
		tuple := scanner.Text()
		lineNumber++
		lineString := strings.Split(tuple, "__")[1]
		line := fmt.Sprintf("%d__%s", lineNumber, lineString)
		log.Printf("passing %d:%s to tupleChannel\n", lineNumber, line)
		tupleChannel <- line
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading input stream: %v\n", err)
		return
	}
	close(tupleChannel)
	wg.Wait()
}

func buildBinaries(op1_exe string, op2_exe string) {
	// build op1_exe
	cmd1 := exec.Command("go", "build", "-o", "./ops/"+op1_exe, "./ops/"+op1_exe+".go")
	cmd2 := exec.Command("go", "build", "-o", "./ops/"+op2_exe, "./ops/"+op2_exe+".go")

	if _, err := cmd1.Output(); err != nil {
		fmt.Printf("Error executing cmd1: %v\n", err)
	}

	if _, err := cmd2.Output(); err != nil {
		fmt.Printf("Error executing cmd2: %v\n", err)
	}
}

func getMapKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func (s *StreamService) UpdateStreamGroup(args StreamGroupArgs, reply *OperationReply) error {
	fmt.Printf("Update stream groupncalled\n")
	log.Printf("Current stream Group: %v\n", streamGroup)
	sgMutex.Lock()
	streamGroup = args.StreamGroup
	sgMutex.Unlock()
	fmt.Printf("Updated stream Group: %v\n", streamGroup)
	reply.Ack = true
	return nil
}
