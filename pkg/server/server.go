package server

import (
	"bytes"
	"cs425-mp1/pkg/message"
	"encoding/gob"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

// ServeHeartbeat incoming heartbeats
func ServeHeartbeat(c chan map[string]message.Member, JOINED *bool, currMember *message.Member, mutex *sync.Mutex) {
	port := message.NODE_PORT
	if currMember.ID == "INTRODUCER" {
		port = message.INTRO_HB_PORT
	}
	pc, err := net.ListenPacket("udp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}
	defer pc.Close()
	buffer := make([]byte, 2048)
	var members map[string]message.Member
	var query message.Query
	var member message.Member

	for {
		mutex.Lock()
		if !*JOINED {
			mutex.Unlock()
			time.Sleep(1 * time.Second)
			continue
		}
		mutex.Unlock()
		n, addr, err := pc.ReadFrom(buffer)
		if err != nil {
			message.Log("Connection has error: " + err.Error())
			continue
		}
		if err := gob.NewDecoder(bytes.NewReader(buffer[:n])).Decode(&query); err != nil {
			message.Log("HBServer Error")
			log.Fatal(err)
		}
		members = query.Payload
		if query.Header != "INTRO" {
			// update mode
			if currMember.Mode != query.Mode && currMember.ModeTs < query.ModeTs {
				currMember.Mode = query.Mode
				currMember.ModeTs = query.ModeTs
				message.Log("Mode change to: " + currMember.Mode)
			}
			// refresh sender state
			address := strings.Split(addr.String(), ":")[0]
			member = message.Member{
				ID:        query.ID,
				Addr:      address,
				IsAlive:   true,
				Timestamp: time.Now().Unix(),
				Hc:        query.Hc,
				Mode:      query.Mode,
				ModeTs:    query.ModeTs,
			}
			mutex.Lock()
			members[address] = member
			mutex.Unlock()
			// log.Println("Receive heartbeat from " + address)
		}
		c <- members
	}

}
