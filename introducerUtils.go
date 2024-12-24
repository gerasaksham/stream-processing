package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

var (
	sgMutex        sync.Mutex
	failureChannel = make(chan Update, 100)
)

type StreamGroupArgs struct {
	StreamGroup []string
}

func updateStreamGroup(node string) {
	sgMutex.Lock()

	nextAliveNodeIdx := findAliveSuccessorOfNode(getRingIndex(node))
	nextAliveNode := listOfHashes[nextAliveNodeIdx].Name

	for i, n := range streamGroup {
		if n == node {
			streamGroup[i] = nextAliveNode
			break
		}
	}

	sgMutex.Unlock()

	for _, node := range streamGroup {
		client, err := rpc.Dial("tcp", makeAddrPort(node, 8001))
		if err != nil {
			fmt.Printf("Error dialing to node %s: %s\n", node, err)
			return
		}
		var reply OperationReply
		args := StreamGroupArgs{StreamGroup: streamGroup}
		err = client.Call("StreamService.UpdateStreamGroup", args, &reply)
		client.Close()
		if err != nil {
			fmt.Printf("Error updating stream group on node %s: %s\n", node, err)
		}
	}
	fmt.Printf("Assigning task to next alive node: %s\n", nextAliveNode)
	currentRingIdx = getRingIndex(selfAddr)
	nextAliveSuccessor := findAliveSuccessorOfNode(currentRingIdx)
	secondAliveSuccessor := findAliveSuccessorOfNode(nextAliveSuccessor)
	firstReplicaStreamGroup = listOfHashes[nextAliveSuccessor].Name
	secondReplicaStreamGroup = listOfHashes[secondAliveSuccessor].Name
	specialNode := nextAliveNode + "$" + node
	startStagesAcrossNodes(specialNode, op1_exe, op2_exe, currentTask, currentDestFile, currentPattern)
}

func handleUpdate(update Update) {
	mutex.Lock()
	defer mutex.Unlock()
	if update.Status == 0 {
		if !susEnabled {
			if _, ok := membershipList[update.Node]; !ok {
				membershipList[update.Node] = 0
			}
		} else {
			if _, ok := membershipList[update.Node]; !ok {
				if update.IncarnationNumber > incarnationNumbers[update.Node] && update.IncarnationNumber != 2 {
					membershipList[update.Node] = 0
					incarnationNumbers[update.Node] = update.IncarnationNumber
				}
			}
		}
	} else if update.Status == 1 {
		if susEnabled {
			if _, ok := membershipList[update.Node]; !ok {
				if update.IncarnationNumber >= incarnationNumbers[update.Node] && update.IncarnationNumber != 2 {
					membershipList[update.Node] = 1
					incarnationNumbers[update.Node] = update.IncarnationNumber
				}
			}
		}
	} else if update.Status == 2 {
		// check if the node is in the membership list
		if _, ok := membershipList[update.Node]; !ok {
			return
		}

		failureChannel <- update
	}
}

func failureHandlerThread() {
	for {
		time.Sleep(15 * time.Second)
		var updates []Update
		for {
			select {
			case update := <-failureChannel:
				updates = append(updates, update)
			default:
				// Exit the loop when no more updates are available
				goto done
			}
		}
	done:
		if len(updates) > 0 {
			processUpdates(updates)
		}
	}
}

func removeDuplicateUpdates(updates []Update) []Update {
	uniqueUpdates := make(map[string]Update)
	for _, update := range updates {
		uniqueUpdates[update.Node] = update
	}
	result := make([]Update, 0, len(uniqueUpdates))
	for _, update := range uniqueUpdates {
		result = append(result, update)
	}
	return result
}

func processUpdates(updates []Update) {
	mutex.Lock()
	defer mutex.Unlock()
	var node0 string
	var node1 string
	var nextAliveNodeIdx0 int
	var nextAliveNode0 string
	var nextAliveNodeIdx1 int
	var nextAliveNode1 string
	fmt.Printf("Processing failed updates\n")

	// delete any duplicate updates
	updates = removeDuplicateUpdates(updates)

	if len(updates) == 2 {
		fmt.Printf("Two nodes failed\n")
		for _, update := range updates {
			fmt.Printf("Received failed update of node: %s\n", update.Node)
			if _, ok := membershipList[update.Node]; !ok {
				continue
			}
			membershipList[update.Node] = 2
			// find node in list of hashes and mark it as failed
			for i, n := range listOfHashes {
				if n.Name == update.Node {
					listOfHashes[i].Alive = false
					break
				}
			}
		}
		node0 = updates[0].Node
		node1 = updates[1].Node
		node0Idx := getRingIndex(node0)
		node1Idx := getRingIndex(node1)
		if node0Idx != -1 {
			nextAliveNodeIdx0 = findAliveSuccessorOfNode(node0Idx)
			nextAliveNode0 = listOfHashes[nextAliveNodeIdx0].Name
		}
		if node1Idx != -1 {
			nextAliveNodeIdx1 = findAliveSuccessorOfNode(node1Idx)
			nextAliveNode1 = listOfHashes[nextAliveNodeIdx1].Name
		}
		// now update the stream group
		if len(updates) == 2 {
			sgMutex.Lock()

			if nextAliveNode0 != "" {
				for i, n := range streamGroup {
					if n == node0 {
						streamGroup[i] = nextAliveNode0
						break
					}
				}
			}

			if nextAliveNode1 != "" {
				for i, n := range streamGroup {
					if n == node1 {
						streamGroup[i] = nextAliveNode1
						break
					}
				}
			}

			sgMutex.Unlock()

			for _, node := range streamGroup {
				client, err := rpc.Dial("tcp", makeAddrPort(node, 8001))
				if err != nil {
					fmt.Printf("Error dialing to node %s: %s\n", node, err)
					return
				}
				var reply OperationReply
				args := StreamGroupArgs{StreamGroup: streamGroup}
				err = client.Call("StreamService.UpdateStreamGroup", args, &reply)
				client.Close()
				if err != nil {
					fmt.Printf("Error updating stream group on node %s: %s\n", node, err)
				}
			}
			fmt.Printf("Assigning task to next alive nodes: %s, %s\n", nextAliveNode0, nextAliveNode1)
			currentRingIdx = getRingIndex(selfAddr)
			nextAliveSuccessor := findAliveSuccessorOfNode(currentRingIdx)
			secondAliveSuccessor := findAliveSuccessorOfNode(nextAliveSuccessor)
			firstReplicaStreamGroup = listOfHashes[nextAliveSuccessor].Name
			secondReplicaStreamGroup = listOfHashes[secondAliveSuccessor].Name
			specialNode0 := nextAliveNode0 + "$" + node0
			startStagesAcrossNodes(specialNode0, op1_exe, op2_exe, currentTask, currentDestFile, currentPattern)
			specialNode1 := nextAliveNode1 + "$" + node1
			startStagesAcrossNodes(specialNode1, op1_exe, op2_exe, currentTask, currentDestFile, currentPattern)
		}

		for _, update := range updates {
			rebalanceFilesAfterNodeFailure(update.Node)
			for i, node := range listOfNodes {
				if node == update.Node {
					listOfNodes = append(listOfNodes[:i], listOfNodes[i+1:]...)
					break
				}
			}
			log.Printf("Updated membership list: %v\n", membershipList)
			log.Printf("Updated list of nodes: %v\n", listOfNodes)
		}
	} else {
		update := updates[0]
		fmt.Printf("Received failed update of node: %s\n", update.Node)
		if _, ok := membershipList[update.Node]; !ok {
			return
		}
		membershipList[update.Node] = 2
		// find node in list of hashes and mark it as failed
		for i, n := range listOfHashes {
			if n.Name == update.Node {
				listOfHashes[i].Alive = false
				break
			}
		}
		log.Printf("listofhashes: %v\n", listOfHashes)
		updateStreamGroup(updates[0].Node)
		rebalanceFilesAfterNodeFailure(update.Node)
		for i, node := range listOfNodes {
			if node == update.Node {
				listOfNodes = append(listOfNodes[:i], listOfNodes[i+1:]...)
				break
			}
		}
		log.Printf("Updated membership list: %v\n", membershipList)
		log.Printf("Updated list of nodes: %v\n", listOfNodes)
		return
	}
}

func introducerBroadcastListener() {
	addr, err := net.ResolveUDPAddr("udp", ":"+strconv.Itoa(BroadPort))
	if err != nil {
		log.Printf("Error resolving UDP address: %v\n", err)
		return
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Printf("Error listening to udp updates: %v\n", err)
		return
	}
	defer conn.Close()

	buf := make([]byte, 1024)
	for {
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			log.Printf("Error reading udp packet: %v\n", err)
			continue
		}
		if n == 0 {
			continue
		}
		var message Message
		err = Unmarshal(buf[:n], &message)
		if err != nil {
			log.Printf("Error unmarshalling message: %v\n", err)
			continue
		}
		if message.Type == Broadcast {
			update := message.Update
			handleUpdate(update)
		} else if message.Type == EnableSus {
			susEnabled = true
		} else if message.Type == DisableSus {
			susEnabled = false
		} else if message.Type == ChangeDropRate {
			dropRate = message.Update.DropRate
		}
	}
}

type IntroducerService struct{}

func (fs *IntroducerService) Join(joinRequest JoinRequest, introduction *Introduction) error {
	mutex.Lock()
	defer mutex.Unlock()
	fmt.Printf("Join request received from: %v\n", joinRequest.Addr)
	log.Printf("Join request received from: %v\n", joinRequest.Addr)
	newNodeAddress := joinRequest.Addr
	newNodeAddress += "|" + strconv.FormatInt(time.Now().Unix(), 10)
	// newListOfNodes := append(listOfNodes, selfHostName)
	newListOfNodes := []string{}
	for _, node := range listOfNodes {
		newListOfNodes = append(newListOfNodes, node)
	}
	newListOfNodes = append(newListOfNodes, selfAddr)
	membershipList[newNodeAddress] = 0
	listOfNodes = append(listOfNodes, newNodeAddress)
	incarnationNumbers[newNodeAddress] = 0
	listOfHashes = append(listOfHashes, ListNode{Name: newNodeAddress, Hash: HashString(newNodeAddress), Alive: true})
	sortListOfHashes()
	introductionObject := Introduction{MembershipList: membershipList, SelfAddress: newNodeAddress, ListOfNodes: newListOfNodes, IncarnationNumbers: incarnationNumbers, SusEnabled: susEnabled, DropRate: dropRate, IntroducerAddr: selfAddr}
	*introduction = introductionObject
	go informAllNodes(Update{Node: newNodeAddress, Status: 0, IncarnationNumber: 0}, selfHostName)
	fmt.Printf("New node joined: %s\n", newNodeAddress)
	return nil
}
