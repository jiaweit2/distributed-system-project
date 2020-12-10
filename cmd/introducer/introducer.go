package main

import (
	"bytes"
	"cs425-mp1/pkg/client"
	"cs425-mp1/pkg/message"
	"cs425-mp1/pkg/server"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// Address of introducer
var currentMachine = "fa20-cs425-g40-01.cs.illinois.edu"

// membership list
var members = make(map[string]message.Member)

// master
var master = "UNASSIGNED"

var mutex sync.Mutex

func handleJoin(data []byte, addr string) {
	var query message.Query
	// Log the query and construct a Member object
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&query); err != nil {
		log.Fatal(err)
	}
	addr = strings.Split(addr, ":")[0]
	members[addr] = message.Member{
		ID:        fmt.Sprintf("%d:%s", time.Now().Unix(), addr),
		Addr:      addr,
		IsAlive:   true,
		Timestamp: time.Now().Unix(),
		Hc:        query.Hc,
		Mode:      query.Mode,
		ModeTs:    0,
	}
	log.Println(query.String())

	// Repack the new member's address to query
	query.Addr = addr
	query.Payload = members
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(query); err != nil {
		log.Fatal(err)
	}

	// Assign master
	if master == "UNASSIGNED" {
		mutex.Lock()
		master = addr
		mutex.Unlock()
	}
	req := message.Request{
		Header:  "MASTER",
		Payload: master,
	}

	// Send to current members
	for _, member := range members {
		if !member.IsAlive || member.ID == "INTRODUCER" {
			continue
		}
		if member.Addr != addr {
			send(member.Addr+":"+message.NODE_PORT, buf.Bytes())
		}
		conn, err := client.Send(member.Addr, req)
		if err == nil {
			conn.Close()
		}

	}
}

func main() {
	currentMember := message.Member{
		ID:        "INTRODUCER",
		Addr:      currentMachine,
		IsAlive:   true,
		Timestamp: time.Now().Unix(),
		Hc:        0,
		Mode:      "ALL",
		ModeTs:    0,
	}
	members["INTRODUCER"] = currentMember
	memChan := make(chan map[string]message.Member, 1024)
	JOINED := true
	go server.ServeHeartbeat(memChan, &JOINED, &currentMember, &mutex)
	go message.MergeMembers(memChan, members, "INTRODUCER", &mutex)

	pc, err := net.ListenPacket("udp", ":"+message.INTRO_PORT)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Introducer is running...")
	defer pc.Close()

	go checkAlive()

	for {
		buf := make([]byte, 1024)
		n, addr, err := pc.ReadFrom(buf)
		if err != nil {
			log.Printf("Connection has error: %s\n", err)
			continue
		}
		go func() {
			handleJoin(buf[:n], addr.String())
			// send back memberships
			deadline := time.Now().Add(1 * time.Second)
			err = pc.SetWriteDeadline(deadline)
			if err != nil {
				log.Fatal(err)
			}
			var resBuf bytes.Buffer
			if err := gob.NewEncoder(&resBuf).Encode(members); err != nil {
				log.Fatal("Error in encoding memberships\n", err)
			}
			_, err = pc.WriteTo(resBuf.Bytes(), addr)
			if err != nil {
				log.Fatal(err)
			}
		}()
	}
}

//	Helper functions

func checkAlive() {
	for {
		time.Sleep(1 * time.Second) // check membership list every second
		timeNow := time.Now().Unix()
		for ip, mem := range members {
			// don't check whether youself is alive
			if ip != "INTRODUCER" {
				if mem.IsAlive && timeNow-mem.Timestamp > message.TIMEOUT {
					mem.IsAlive = false
					members[ip] = mem
					message.Log("Member " + ip + " is marked as failed")
					if ip == master {
						mutex.Lock()
						master = "UNASSIGNED"
						mutex.Unlock()
						go electMaster()
					}
				}
				if !mem.IsAlive && timeNow-mem.Timestamp > message.CLEANUP {
					delete(members, ip)
					message.Log("Member " + ip + " is deleted from membership list")
				}
			}
		}
	}
}

// Pick the first alive node as the master
func electMaster() {
	for ip, mem := range members {
		if mem.IsAlive && mem.ID != "INTRODUCER" {
			mutex.Lock()
			master = ip
			mutex.Unlock()
			break
		}
	}
	req := message.Request{
		Header:  "MASTER",
		Payload: master,
	}
	for _, member := range members {
		if !member.IsAlive || member.ID == "INTRODUCER" {
			continue
		}
		client.Send(member.Addr, req)
	}
}

func send(addr string, buf []byte) {
	dst, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, dst)
	if err != nil {
		log.Println(err)
		return
	}
	_, err = conn.Write(buf)
	if err != nil {
		log.Println(err)
		return
	}
}

func dns_translate(addr string) string {
	res, err := exec.Command("dig", addr, "+short").Output()
	if err != nil {
		log.Fatal(err)
	}
	return string(res)
}
