package client

import (
	"bytes"
	"cs425-mp1/pkg/message"
	"encoding/gob"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

// Throughput of hearbeat in bytes
var Throughput int64

var mutex = &sync.Mutex{}

// GOSSIPRATE gossip rate
var GOSSIPRATE int = 3

// HeartBeat sends heartbeat to designated address
func HeartBeat(query message.Query, LossRate int) (map[string]message.Member, error) {
	if query.Header != "INTRO" {
		if query.ID == "INTRODUCER" {
			query.Addr += ":" + message.INTRO_HB_PORT
		} else {
			query.Addr += ":" + message.NODE_PORT
		}
		// Simulate packet drop
		rand.Seed(time.Now().UTC().UnixNano())
		if rand.Intn(100) < LossRate {
			log.Println("----PACKET DROP----")
			return nil, nil
		}
	}
	raddr, err := net.ResolveUDPAddr("udp", query.Addr)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	mutex.Lock()
	err = encoder.Encode(query)
	mutex.Unlock()
	if err != nil {
		log.Fatal(err)
	}
	mutex.Lock()
	Throughput += int64(buffer.Len())
	mutex.Unlock()
	conn.Write(buffer.Bytes())
	// log.Println("Heartbeat sent to " + query.Addr)

	if query.Header == "INTRO" {
		// wait for a membership list
		deadline := time.Now().Add(3 * time.Second)
		err = conn.SetReadDeadline(deadline)
		if err != nil {
			log.Fatal(err)
		}
		var members map[string]message.Member
		buf := make([]byte, 1024)
		n, _, err := conn.ReadFrom(buf)
		if err == nil {
			// node responds in time, then merge the received membership list
			if err := gob.NewDecoder(bytes.NewReader(buf[:n])).Decode(&members); err == nil {
				return members, nil
			}
		}
		return nil, err
	}
	return nil, nil
}

// SendHeartbeat periodically
func SendHeartbeat(currMember message.Member, JOINED bool, members map[string]message.Member, interval int, LossRate int) {
	// local heartbeat counter
	heartbeatCnt := 1
	for {
		time.Sleep(time.Duration(interval) * time.Second)
		mutex.Lock()
		if !JOINED {
			mutex.Unlock()
			continue
		}
		mutex.Unlock()
		toGossip := make([]string, len(members))
		if currMember.Mode == "G" {
			for k := range members {
				toGossip = append(toGossip, k)
			}
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(len(toGossip), func(i, j int) { toGossip[i], toGossip[j] = toGossip[j], toGossip[i] })
		}
		cnt := 0
		if currMember.Mode == "G" {
			// Gossip mode
			for _, ip := range toGossip {
				if cnt >= GOSSIPRATE {
					break
				}
				if members[ip].IsAlive {
					query := message.Query{
						ID:      members[ip].ID,
						Header:  "HB",
						Addr:    members[ip].Addr,
						Hc:      heartbeatCnt,
						Mode:    currMember.Mode,
						ModeTs:  currMember.ModeTs,
						Payload: members,
					}
					go HeartBeat(query, LossRate)
					cnt++
				}
			}
		} else {
			// ALL to ALL mode
			for _, mem := range members {
				if mem.IsAlive {
					query := message.Query{
						ID:      mem.ID,
						Header:  "HB",
						Addr:    mem.Addr,
						Hc:      heartbeatCnt,
						Mode:    currMember.Mode,
						ModeTs:  currMember.ModeTs,
						Payload: members,
					}
					go HeartBeat(query, LossRate)
				}
			}
		}
		heartbeatCnt++
	}
}
