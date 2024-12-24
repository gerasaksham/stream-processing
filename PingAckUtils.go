package main

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"time"
)

func susConfirmation(timeoutTime time.Time, targetAddr string, selfAddr string) {
	for {
		if time.Now().After(timeoutTime) {
			if membershipList[targetAddr] == 1 {
				fmt.Printf("Suspected node %s did not respond; marking as expired at %s\n", targetAddr, time.Now())
				log.Printf("Suspected node %s did not respond; marking as expired\n", targetAddr)
				markNodeAsFailed(targetAddr)
				update := Update{Node: targetAddr, Status: 2}
				informAllNodes(update, selfAddr)
			}
			return
		}
	}
}

func sendPing(selfAddr string, targetAddr string, pingMessage PingMessage) {

	if shouldDropMessage() {
		log.Printf("Dropping ping message from %s to %s\n", selfAddr, targetAddr)
		return
	}

	// Remove the timestamps and attach the ports
	sourceAddr := makeAddrPort(selfAddr, AckPort)
	destAddr := makeAddrPort(targetAddr, PingPort)
	timeout := 10 * time.Second

	udpDestAddr, err := net.ResolveUDPAddr("udp", destAddr)
	if err != nil {
		log.Printf("error resolving UDP address: %v", err)
		return
	}

	// Bind the client to a specific port
	udpSourceAddr, err := net.ResolveUDPAddr("udp", sourceAddr)
	if err != nil {
		log.Printf("error resolving local UDP address: %v", err)
		return
	}

	// Create a UDP connection
	conn, err := net.DialUDP("udp", nil, udpDestAddr)

	if err != nil {
		log.Printf("error creating UDP connection: %v", err)
		return
	}
	defer conn.Close()
	// Marshal the ping message
	data, err := Marshal(pingMessage)
	if err != nil {
		log.Printf("error marshaling ping message: %v", err)
		return
	}

	_, err = conn.Write(data)
	if err != nil {
		log.Printf("Error sending ping message: %v\n", err)
		return
	}

	if getHostnameFromNodename(targetAddr) != IntroducerAddr { // Set the timeout for receiving acknowledgment
		deadline := time.Now().Add(timeout)
		conn1, err := net.ListenUDP("udp", udpSourceAddr)
		if err != nil {
			log.Printf("Error creating UDP listener: %v\n", err)
			return
		}
		defer conn1.Close()
		err = conn1.SetDeadline(deadline)
		if err != nil {
			log.Printf("Error setting deadline: %v\n", err)
			return
		}

		// Wait for acknowledgment
		buffer := make([]byte, 1024)
		n, _, err := conn1.ReadFromUDP(buffer)
		//bytesSyntax.Lock()
		//numOfBytes += n
		//bytesSyntax.Unlock()

		if err != nil || n == 0 {
			if netErr, ok := err.(net.Error); ok {
				if netErr.Timeout() {
					log.Printf("Timeout waiting for acknowledgment from %s\n", targetAddr)
				}
			} else {
				log.Printf("Error reading acknowledgment: %v\n", err)
			}
			if !susEnabled {
				markNodeAsFailed(targetAddr)
				update := Update{Node: targetAddr, Status: 2}
				informAllNodes(update, selfAddr)
				log.Printf("Failure Detected for %s at %s\n", targetAddr, time.Now())
				fmt.Printf("Failure Detected for %s at %s\n", targetAddr, time.Now())
				return
			} else {
				if !(pingMessage.Suspected) {
					mutex.Lock()
					if membershipList[targetAddr] == 0 {
						membershipList[targetAddr] = 1
					}
					mutex.Unlock()
					fmt.Printf("Marked %s as suspected\n", targetAddr)
					log.Printf("Marked %s as suspected\n", targetAddr)
					go informAllNodes(Update{Node: targetAddr, Status: 1, IncarnationNumber: incarnationNumbers[targetAddr]}, selfAddr)
					timeoutTime := time.Now().Add(10 * time.Second)
					go susConfirmation(timeoutTime, targetAddr, selfAddr)
				}
			}
			return
		}

		// Received acknowledgment
		if susEnabled {
			if pingMessage.Suspected {
				mutex.Lock()
				membershipList[targetAddr] = 0
				mutex.Unlock()
				informAllNodes(Update{Node: targetAddr, Status: 0, IncarnationNumber: incarnationNumbers[targetAddr]}, selfAddr)
			}
		}
	}
}

func broadcastListener() {
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
	udpConnBroadcast = conn // Store the connection in the global variable
	defer conn.Close()

	buf := make([]byte, 1024)
	for {
		select {
		case <-stopChan:
			return
		default:
			n, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				log.Printf("Error reading udp packet: %v\n", err)
				continue
			}
			if n == 0 {
				continue
			}
			var message Message
			_ = Unmarshal(buf[:n], &message)
			if message.Type == Broadcast {
				update := message.Update
				log.Printf("Received broadcast message: %v\n", update)

				if update.Node == selfAddr {
					if susEnabled && update.Status == 1 {
						mutex.Lock()
						membershipList[update.Node] = 0
						incarnationNumbers[update.Node]++
						mutex.Unlock()
						informAllNodes(Update{Node: update.Node, Status: 0, IncarnationNumber: incarnationNumbers[update.Node]}, selfAddr)
						log.Printf("Updated membership list: %v\n", membershipList)
						log.Printf("Updated incarnation numbers: %v\n", incarnationNumbers)
					}
					continue
				}
				if update.Status == 2 {
					markNodeAsFailed(update.Node)
				} else if update.Status == 0 {
					if susEnabled && membershipList[update.Node] == 1 && update.IncarnationNumber > incarnationNumbers[update.Node] {
						mutex.Lock()
						membershipList[update.Node] = 0
						incarnationNumbers[update.Node] = update.IncarnationNumber
						mutex.Unlock()
						log.Printf("Updated membership list: %v\n", membershipList)
						log.Printf("Updated incarnation numbers: %v\n", incarnationNumbers)
					}
				} else if update.Status == 1 {
					if susEnabled && membershipList[update.Node] != 2 {
						if update.IncarnationNumber >= incarnationNumbers[update.Node] {
							mutex.Lock()
							membershipList[update.Node] = 1
							incarnationNumbers[update.Node] = update.IncarnationNumber
							mutex.Unlock()
							log.Printf("Updated membership list: %v\n", membershipList)
							log.Printf("Updated incarnation numbers: %v\n", incarnationNumbers)
						}
					}
				}
			} else if message.Type == Introducer {
				update := message.Update
				addNewNode(update.Node)
			} else if message.Type == EnableSus {
				susEnabled = true
			} else if message.Type == DisableSus {
				susEnabled = false
			} else if message.Type == ChangeDropRate {
				dropRate = message.Update.DropRate
			}
		}
	}
}

func sendAcknowledgment(conn *net.UDPConn, remoteAddr *net.UDPAddr, message Message) error {
	if shouldDropMessage() {
		log.Printf("Dropping acknowledgment message to %s\n", remoteAddr)
		return nil
	}

	data, err := Marshal(message)
	if err != nil {
		log.Printf("error marshaling acknowledgment message: %v", err)
		return fmt.Errorf("error marshaling acknowledgment message: %v", err)
	}
	_, err = conn.WriteToUDP(data, remoteAddr)
	if err != nil {
		log.Printf("error sending acknowledgment: %v", err)
		return fmt.Errorf("error sending acknowledgment: %v", err)
	}

	return nil
}

func startNode(nodeAddr string) {
	nodeAddr = makeAddrPort(nodeAddr, PingPort)
	udpAddr, err := net.ResolveUDPAddr("udp", nodeAddr)
	if err != nil {
		log.Printf("Error resolving UDP address: %v\n", err)
		return
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Printf("Error creating UDP listener: %v\n", err)
		return
	}
	udpConnPing = conn // Store the connection in the global variable

	// Channel to receive packets

	packetChannel := make(chan Packet, 100)

	go func() {
		defer conn.Close()
		for {
			select {
			case <-stopChan:
				return
			default:
				buffer := make([]byte, 1024)
				n, _, err := conn.ReadFromUDP(buffer)
				bytesSyntax.Lock()
				//numOfBytes += n
				bytesSyntax.Unlock()
				if err != nil {
					log.Printf("Error reading from UDP: %v\n", err)
					continue
				}
				packetChannel <- Packet{Buffer: buffer[:n]}
			}
		}
	}()

	// Goroutine to process packets from the channel
	go func() {
		for packet := range packetChannel {
			var message PingMessage
			err := Unmarshal(packet.Buffer, &message)
			if err != nil {
				log.Printf("message before error: %v\n", packet.Buffer)
				log.Printf("Error unmarshalling message: %v\n", err)
				continue
			}

			if susEnabled && message.Suspected {
				mutex.Lock()
				incarnationNumbers[selfAddr]++
				mutex.Unlock()
				informAllNodes(Update{Node: selfAddr, Status: 0, IncarnationNumber: incarnationNumbers[selfAddr]}, selfAddr)
			}
			var ackMessage Message
			ackMessage.Source = selfAddr
			ackMessage.Type = Ack
			targetAddr := makeAddrPort(message.Source, AckPort)
			destAddr, err := net.ResolveUDPAddr("udp", targetAddr)
			if err != nil {
				log.Printf("Error resolving UDP address: %v\n", err)
				continue
			}
			err = sendAcknowledgment(conn, destAddr, ackMessage)
			if err != nil {
				log.Printf("Error sending acknowledgment: %v\n", err)
			}
		}
	}()
}

func periodicPing() {
	for {
		select {
		case <-stopChan:
			return
		default:
			targetAddr, error := getNextTarget()
			if error != nil {
				log.Printf("No ping target defined for node %s\n", nodeAddr)
				time.Sleep(5 * time.Second)
				continue
			}
			suspected := false
			if susEnabled && membershipList[targetAddr] == 1 {
				suspected = true
			}
			if membershipList[targetAddr] == 2 {
				continue
			}
			pingMessage := PingMessage{Source: selfAddr, Suspected: suspected}

			sendPing(selfAddr, targetAddr, pingMessage)
			time.Sleep(1200 * time.Millisecond)
		}
	}
}

func informAllNodes(update Update, selfHostName string, msgType ...string) {

	var messageType string
	if len(msgType) > 0 {
		messageType = msgType[0]
	} else {
		if isSelfIntroducer {
			messageType = Introducer
		} else {
			messageType = Broadcast
		}
	}

	for _, node := range listOfNodes {
		if update.Node == node {
			continue
		}
		targetAddress := getHostnameFromNodename(node) + ":" + strconv.Itoa(BroadPort)
		if shouldDropMessage() {
			log.Printf("Dropping broadcast message to %s\n", targetAddress)
			continue
		}
		resolveAddr, err := net.ResolveUDPAddr("udp", targetAddress)
		if err != nil {
			log.Printf("Error resolving udp address: %v\n", err)
			return
		}
		conn, err := net.DialUDP("udp", nil, resolveAddr)
		defer conn.Close()
		if err != nil {
			log.Printf("Error dialing udp connection: %v\n", err)
			return
		}
		message := Message{Type: messageType, Source: selfHostName, Update: update}
		data, err := Marshal(message)
		if err != nil {
			log.Printf("Error marshalling update: %v\n", err)
			return
		}
		conn.Write(data)
	}
}

func leaveCluster() {

	// Signal goroutines to stop
	close(stopChan)

	// Close the UDP listener connection
	if udpConnBroadcast != nil {
		udpConnBroadcast.Close()
	}
	if udpConnPing != nil {
		udpConnPing.Close()
	}

	// Clean up resources
	log.Printf("Node %s has left the cluster.\n", selfAddr)
}
