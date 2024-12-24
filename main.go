package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	listOfNodes        = []string{}
	listOfHashes       = []ListNode{}
	nextTarget         = 0
	mutex              sync.Mutex
	membershipList     = make(map[string]int)
	incarnationNumbers = make(map[string]int)
	fileMap            = make(map[string]int)
	fileCounter        = make(map[string]int)
	nodeAddr           string
	selfAddr           string
	susEnabled         = true
	stopChan           = make(chan struct{})
	udpConnPing        *net.UDPConn
	udpConnBroadcast   *net.UDPConn
	dropRate           = 0
	logFile            *os.File
	//numOfBytes         = 0
	bytesSyntax      sync.Mutex
	isSelfIntroducer bool
	selfHostName     string
	streamGroup      = []string{}
	op1_exe          string
	op2_exe          string
)

type ListNode struct {
	Name  string
	Hash  uint32
	Alive bool
}

type ServerNode struct {
	Name  string
	Files []File
}

type File struct {
	FileName string
	Owner    bool
}

type multiAppendArgs struct {
	FileName  string
	VMFileMap map[string]string
}

const (
	Ping           string = "ping"
	Ack            string = "ack"
	PingPort       int    = 8080
	AckPort        int    = 8081
	BroadPort      int    = 8082
	AppendPort     int    = 8000
	Introducer     string = "Introducer"
	IntroducerAddr string = "fa24-cs425-3107.cs.illinois.edu"
	Broadcast      string = "Broadcast"
	EnableSus      string = "EnableSus"
	DisableSus     string = "DisableSus"
	ChangeDropRate string = "ChangeDropRate"
)

func init() {
	// Create a log file
	var err error
	logFile, err = os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Printf("Error creating log file: %v\n", err)
		os.Exit(1)
	}

	// Set log output to the file
	log.SetOutput(logFile)
	rand.Seed(time.Now().UnixNano())
}

// Updates about nodes
type Update struct {
	Node              string
	Status            int // 0 for alive, 1 for suspected, 2 for dead
	DropRate          int
	IncarnationNumber int
}

type JoinRequest struct {
	Addr string
}

type Packet struct {
	Buffer []byte
}

type Message struct {
	Type   string
	Source string
	Update Update
}

type PingMessage struct {
	Source    string
	Suspected bool
}

type Introduction struct {
	MembershipList     map[string]int // include self and introducer
	IncarnationNumbers map[string]int
	SelfAddress        string
	ListOfNodes        []string // don't include self
	SusEnabled         bool
	DropRate           int
	IntroducerAddr     string
}

type AppendRequest struct {
	LocalFileName string
	HyDFSFileName string
}

// func logBandwidth() {
// 	for {
// 		timeTenSecondsFromNow := time.Now().Add(10 * time.Second)
// 		for time.Now().Before(timeTenSecondsFromNow) {
// 			continue
// 		}
// 		// Print float value of NumofBytes divied by 10
// 		// Reset NumofBytes to 0
// 		bytesSyntax.Lock()
// 		bandwidth := float64(numOfBytes) / 10
// 		numOfBytes = 0
// 		bytesSyntax.Unlock()
// 		fmt.Printf("Bandwidth: %f\n", bandwidth)
// 	}
// }

func main() {
	// Make channel for append requests
	appendRequests := make(chan AppendRequest, 1500)
	// Start a goroutine to handle append requests
	go func() {
		for req := range appendRequests {
			// fmt.Printf("Appending file %s to server %s\n", req.LocalFileName, req.HyDFSFileName)
			err := appendFileOnServer(req.LocalFileName, req.HyDFSFileName)
			if err != nil {
				log.Printf("Error appending file on server: %v\n", err)
			}
		}
	}()

	var err error
	selfHostName, err = os.Hostname()
	if err != nil {
		log.Printf("Error getting hostname: %v\n", err)
		return
	}

	if selfHostName == IntroducerAddr {
		isSelfIntroducer = true
	}

	fileService := NewFileService()
	fileMergeService := new(FileMergeService)
	fileWriteService := new(FileWriteService)
	fileReadService := new(FileReadService)
	fileWriteFromReplicaService := new(FileWriteFromReplicaService)
	streamService := new(StreamService)
	rpc.Register(fileMergeService)
	rpc.Register(fileService)
	rpc.Register(fileWriteFromReplicaService)
	rpc.Register(fileWriteService)
	rpc.Register(fileReadService)
	rpc.Register(streamService)

	if isSelfIntroducer {
		introducerService := new(IntroducerService)
		rpc.Register(introducerService)
		go introducerBroadcastListener()
		fmt.Printf("starting introducer\n")
		selfAddr = IntroducerAddr + "|" + strconv.FormatInt(time.Now().Unix(), 10)
		listOfHashes = append(listOfHashes, ListNode{Name: selfAddr, Hash: HashString(selfAddr), Alive: true})
		go failureHandlerThread()
	}

	listener, err := net.Listen("tcp", ":8000")
	if err != nil {
		log.Fatal("Listener error:", err)
	}
	getListener, getErr := net.Listen("tcp", ":8001")
	if getErr != nil {
		fmt.Println("Error starting server:", getErr)
		log.Fatal("Listener error:", getErr)
	}

	// listening on Port 8001
	go func() {
		for {
			conn, err := getListener.Accept()
			if err != nil {
				fmt.Println("Connection error:", err)
				log.Println("Connection error:", err)
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()

	// listening on Port 8000
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Println("Connection error:", err)
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()

	// Delete contents of ./hydfs/ directory
	entries, err := os.ReadDir("./hydfs/")
	if err != nil {
		log.Printf("Error reading hydfs directory: %v\n", err)
		return
	}

	for _, entry := range entries {
		err = os.RemoveAll("./hydfs/" + entry.Name())
		if err != nil {
			log.Printf("Error removing %s: %v\n", entry.Name(), err)
		}
	}

	clientCache := Cache{cacheMap: make(map[string]CacheValue), byteCapacity: 1024 * 1024 * 2, numberOfBytesUsed: 0}

	reader := bufio.NewReader(os.Stdin)
	for {
		userInput, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading input: %v\n", err)
			return
		}
		if userInput == "join\n" {
			if isSelfIntroducer {
				fmt.Printf("Introducer cannot join the cluster\n")
				continue
			}
			stopChan = make(chan struct{}) // Reinitialize the stop channel
			conn, err := net.Dial("tcp", IntroducerAddr+":8000")
			if err != nil {
				log.Printf("Error connecting to introducer: %v\n", err)
				return
			}
			client := rpc.NewClient(conn)
			joinRequest := JoinRequest{Addr: selfHostName}
			introduction := Introduction{}
			err = client.Call("IntroducerService.Join", joinRequest, &introduction)
			if err != nil {
				log.Printf("Error joining cluster: %v\n", err)
				return
			}
			mutex.Lock()
			listOfNodes = introduction.ListOfNodes
			for _, node := range listOfNodes {
				listOfHashes = append(listOfHashes, ListNode{Name: node, Hash: HashString(node), Alive: true})
			}
			membershipList = introduction.MembershipList
			selfAddr = introduction.SelfAddress
			listOfHashes = append(listOfHashes, ListNode{Name: selfAddr, Hash: HashString(selfAddr), Alive: true})
			incarnationNumbers = introduction.IncarnationNumbers
			susEnabled = introduction.SusEnabled
			dropRate = introduction.DropRate
			mutex.Unlock()
			sortListOfHashes()

			go periodicPing()
			go startNode(selfHostName)
			go broadcastListener()
			// go logBandwidth()
		} else if userInput == "list_self\n" {
			fmt.Printf("Self address: %s\n", selfAddr)
		} else if userInput == "list_mem\n" {
			prettyPrintMap(membershipList)
		} else if userInput == "enable_sus\n" {
			susEnabled = true
			informAllNodes(Update{}, selfAddr, EnableSus)
		} else if userInput == "disable_sus\n" {
			susEnabled = false
			informAllNodes(Update{}, selfAddr, DisableSus)
		} else if userInput == "leave\n" {
			leaveCluster()
			fmt.Printf("Leaving at %s\n", time.Now())
		} else if userInput == "status_sus\n" {
			if susEnabled {
				fmt.Printf("Suspicion is enabled\n")
			} else {
				fmt.Printf("Suspicion is disabled\n")
			}
		} else if strings.HasPrefix(userInput, "change_drop_rate") {
			parts := strings.Split(userInput, " ")
			if len(parts) == 2 {
				newDropRate, err := strconv.Atoi(parts[1][:len(parts[1])-1])
				if err != nil {
					fmt.Printf("Invalid drop rate value: %v\n", err)
					continue
				}
				informAllNodes(Update{DropRate: newDropRate}, selfAddr, ChangeDropRate)
				fmt.Printf("Drop rate changed to %d\n", newDropRate)
			} else {
				fmt.Printf("Invalid command format. Use: change_drop_rate <value>\n")
			}
		} else if strings.HasPrefix(userInput, "create") {
			inputParts := strings.Fields(userInput)
			if len(inputParts) != 3 {
				log.Println("Invalid input format. Expected: createfile srcfile destfile")
				continue
			}
			fmt.Printf("Creating file %s on server %s\n", inputParts[1], inputParts[2])

			srcFile := inputParts[1]
			srcFile = "./local/" + srcFile
			destFile := inputParts[2]
			destFile = "./hydfs/" + destFile
			fmt.Printf("Hash of file: %d\n", HashString(destFile))
			destNode, err := findServerGivenFileHash(HashString(destFile))
			if err != nil {
				log.Printf("Error finding server given file hash: %v\n", err)
				continue
			}
			destNodeName := destNode.Name
			fmt.Printf("Destination node: %s\n", destNodeName)
			//fileCounter[destFile] = 0
			destFile = addCounterToFilename(destFile, 0)
			replicas := getReplicas(destNodeName)

			go func() {
				err = writeFileOnServer(srcFile, destFile, destNodeName, 1)
			}()
			if err != nil {
				log.Printf("Error writing file on server: %v\n", err)
			}

			for _, replica := range replicas {
				go func(replica string) {
					err = writeFileOnServer(srcFile, destFile, replica, 2)
					if err != nil {
						log.Printf("Error writing file on server: %v\n", err)
					}
				}(replica)
			}

			fmt.Printf("File %s created on server %s\n", destFile, destNodeName)

			// Replicate the file on next two successors
			//sendReplicateRequest(destFile, destNodeName)
		} else if strings.HasPrefix(userInput, "getfromreplica") {
			fmt.Printf("Getting file from replica\n")
			inputParts := strings.Fields(userInput)
			if len(inputParts) != 4 {
				fmt.Println("Invalid input format. Expected: getfromreplica servername srcfile destfile")
				log.Println("Invalid input format. Expected: getfromreplica srcfile destfile")
				continue
			}
			localFilename := "./local/" + inputParts[3]
			fmt.Printf("Local filename: %s\n", localFilename)
			hydfsFilename := "./hydfs/" + inputParts[2]
			fmt.Printf("Hydfs filename: %s\n", hydfsFilename)
			serverName := inputParts[1]
			fmt.Printf("Server name: %s\n", serverName)
			hydfsFilename = addCounterToFilename(hydfsFilename, 0)
			err := writeFileFromReplica(localFilename, hydfsFilename, serverName)
			if err != nil {
				fmt.Printf("Error writing file on server: %v\n", err)
				log.Printf("Error writing file on server: %v\n", err)
			}

		} else if strings.HasPrefix(userInput, "get") {
			inputParts := strings.Fields(userInput)
			if len(inputParts) != 3 {
				log.Println("Invalid input format. Expected: readfile filename")
				continue
			}
			// fmt.Printf("Reading file %s\n", inputParts[1])
			hydfsFilename := "./hydfs/" + inputParts[1]
			localFilename := "./local/" + inputParts[2]

			value, boo := clientCache.get(hydfsFilename)
			if boo {
				fmt.Printf("File %s found in cache\n", hydfsFilename)
				err := os.WriteFile(localFilename, []byte(value), 0644)
				if err != nil {
					log.Printf("Error writing file to OS: %v\n", err)
				}
				fmt.Printf("File contents: \n%s\n", value)
				continue
			}

			err := readFileFromServer(hydfsFilename, localFilename)
			if err != nil {
				log.Printf("Error reading file from server: %v\n", err)
			}

			localReadFile, err := os.Open(localFilename)
			if err != nil {
				log.Printf("Error reading file from OS: %v\n", err)
				continue
			}
			defer localReadFile.Close()

			fileContents, err := io.ReadAll(localReadFile)
			if err != nil {
				log.Printf("Error reading file from OS: %v\n", err)
				continue
			}

			clientCache.put(hydfsFilename, string(fileContents))

		} else if userInput == "store\n" {
			// print list of files in hydfs folder
			files, err := os.ReadDir("./hydfs/")
			if err != nil {
				log.Printf("Error reading hydfs directory: %v\n", err)
				continue
			}
			fmt.Printf("Files stored in hydfs:\n")
			for _, file := range files {
				fmt.Printf("%s\n", file.Name())
			}
		} else if strings.HasPrefix(userInput, "append") {
			inputParts := strings.Fields(userInput)
			if len(inputParts) != 3 {
				log.Println("Invalid input format. Expected: appendfile srcfile destfile")
				continue
			}
			localFilename := "./local/" + inputParts[1]
			hydfsFilename := "./hydfs/" + inputParts[2]
			clientCache.remove(hydfsFilename)
			// Send append request to the channel
			appendRequests <- AppendRequest{
				LocalFileName: localFilename,
				HyDFSFileName: hydfsFilename,
			}
		} else if strings.HasPrefix(userInput, "merge") {
			inputParts := strings.Fields(userInput)
			if len(inputParts) != 2 {
				log.Println("Invalid input format. Expected: appendfile srcfile destfile")
				continue
			}
			filename := "./hydfs/" + inputParts[1]
			err := mergeFiles(filename)
			if err != nil {
				log.Printf("Error merging files: %v\n", err)
			}
		} else if userInput == "wait\n" {
			time.Sleep(5 * time.Second)
		} else if strings.HasPrefix(userInput, "ls") {
			inputParts := strings.Fields(userInput)
			if len(inputParts) != 2 {
				log.Println("Invalid input format. Expected: appendfile srcfile destfile")
				continue
			}
			filename := "./hydfs/" + inputParts[1]
			err := listHostNames(filename, inputParts[1])
			if err != nil {
				log.Printf("Error merging files: %v\n", err)
			}
		} else if userInput == "list_mem_ids\n" {
			printRingMembershipList()
		} else if strings.HasPrefix(userInput, "multiappend") {
			inputParts := strings.Fields(userInput)
			numberOfArguments := len(inputParts)
			if (numberOfArguments)%2 != 0 {
				fmt.Printf("Wrong number of arguments\n")
				continue
			}
			if numberOfArguments < 4 {
				fmt.Printf("Invalid command - too few arguments\n")
				continue
			}

			targetFilename := inputParts[1]
			args := multiAppendArgs{FileName: targetFilename, VMFileMap: make(map[string]string)}
			for i := 2; i < numberOfArguments; i += 2 {
				vm := inputParts[i]
				appendFile := inputParts[i+1]
				args.VMFileMap[vm] = appendFile
			}

			err := multiAppendFileOnServer(args)
			if err != nil {
				fmt.Printf("Error appending file on server: %v\n", err)
				log.Printf("Error appending file on server: %v\n", err)
			}

		} else if strings.HasPrefix(userInput, "thousand") {
			// create the 10mb file in hydfs
			// run RPC to append concurrently to that file with a 10kb file
			// specify number of concurrent appends
			inputParts := strings.Fields(userInput)
			numberOfServers, _ := strconv.Atoi(string(inputParts[1]))

			// create the 10mb file
			// srcFile := "./local/business_49.txt"
			// destFile := "./hydfs/10mb.txt"
			dstFileWithoutCounter := "10mb.txt"
			// destNode, _ := findServerGivenFileHash(HashString(destFile))
			// destNodeName := destNode.Name
			// destFile = addCounterToFilename(destFile, 0)
			// replicas := getReplicas(destNodeName)
			// writeFileOnServer(srcFile, destFile, destNodeName, 1)
			// for _, replica := range replicas {
			// 	writeFileOnServer(srcFile, destFile, replica, 2)
			// }

			// rpc stuff
			fileThatIsAppended := "business_51.txt"
			numberOfAppendsPerServer := 1000 / numberOfServers
			for i := 0; i < numberOfServers; i++ {
				serverName := listOfHashes[i]
				if serverName.Alive {
					go runNumberOfAppendsFromServer(fileThatIsAppended, dstFileWithoutCounter, serverName.Name, numberOfAppendsPerServer)
				}
			}

		} else if userInput == "currenttime\n" {
			fmt.Printf("Current time: %s\n", time.Now())
		} else if strings.HasPrefix(userInput, "RainStorm") && isSelfIntroducer {
			inputParts := strings.Fields(userInput)
			if len(inputParts) < 6 {
				log.Println("Invalid input format. Expected: stream filename")
				continue
			}
			op_exe_1 := inputParts[1]
			op_exe_2 := inputParts[2]
			filename := inputParts[3]
			destFileName := inputParts[4]
			num_tasks := inputParts[5]
			var pattern string
			if len(inputParts) > 6 {
				for i := 6; i < len(inputParts)-1; i++ {
					pattern = pattern + inputParts[i] + " "
				}
				pattern = pattern + inputParts[len(inputParts)-1]
			}
			op1_exe = op_exe_1
			op2_exe = op_exe_2
			num_tasks_int, err := strconv.Atoi(num_tasks)
			if err != nil {
				fmt.Printf("Invalid number of tasks: %v\n", err)
				continue
			}
			processJob(op_exe_1, op_exe_2, filename, destFileName, num_tasks_int, pattern)
		} else {
			fmt.Printf("Invalid command\n")
			continue
		}
	}
}
