package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"net"
)

type Node struct {
	addr string // id
	ts   string // local timestamp
	hc   string // heartbeat counter
}

func (n Node) String() string {
	s := "========= Request to join =========="
	s += "Address: " + n.addr + "\n"
	s += "Timestamp: " + n.ts + "\n"
	s += "Heartbeat Counter:" + n.hc + "\n"
	return s
}

func handleJoin(r *bytes.Reader) {
	var node Node
	err := gob.NewDecoder(r).Decode(&node)
	if err != nil {
		log.Printf("Error in decoding message: %s\n", err)
	} else {
		fmt.Println(node.String())
		// TODO: send to current nodes
	}
}

func main() {
	pc, err := net.ListenPacket("udp", ":8000")
	if err != nil {
		log.Fatal(err)
	}
	defer pc.Close()
	for {
		buf := make([]byte, 1024)
		n, _, err := pc.ReadFrom(buf)
		if err != nil {
			log.Printf("Connection has error: %s\n", err)
			continue
		}
		go handleJoin(bytes.NewReader(buf[:n]))
	}
}
