package message

import (
	"cs425-mp1/pkg/fileop"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)

// PORT heartbeat port number
var INTRO_PORT = "8000"
var NODE_PORT = "8001"
var NODE_FILE_PORT = "8002"
var INTRO_HB_PORT = "8003"
var MR_PORT = "8004"

// TIMEOUT for member
var TIMEOUT int64 = 2

// CLEANUP time
var CLEANUP int64 = 4

// Member struct
type Member struct {
	ID        string
	Addr      string
	Hc        int   // heartbeat counter
	Timestamp int64 // number of seconds since January 1, 1970 UTC
	IsAlive   bool
	Mode      string // G or ALL
	ModeTs    int64  // mode last updated time
}

func MergeMembers(memChan <-chan map[string]Member, members map[string]Member, laddr string, mutex *sync.Mutex) {
	for memList := range memChan {
		for ip, newMem := range memList {
			if ip == laddr {
				continue
			}
			if newMem.IsAlive {
				if mem, ok := members[ip]; ok {
					// update old member
					if newMem.Hc > mem.Hc {
						mem.Hc = newMem.Hc
						mem.Timestamp = time.Now().Unix()
						mem.IsAlive = true
						mem.Mode = newMem.Mode
						mem.ModeTs = newMem.ModeTs
						mutex.Lock()
						members[ip] = mem
						mutex.Unlock()
					}
				} else {
					// add new member
					newMem.Timestamp = time.Now().Unix()
					mutex.Lock()
					members[ip] = newMem
					mutex.Unlock()
					Log("New member added: " + ip)
				}
			}
		}
	}
}

func PrintMembers(members map[string]Member) {
	s := "\n"
	for _, mem := range members {
		s += mem.String()
	}
	log.Printf(s)
}

func Log(s string) {
	log.Println(s)
	fmt.Print("-> ")
}

// Query message
type Query struct {
	Header  string // query type
	ID      string
	Hc      int // heartbeat counter
	Addr    string
	Mode    string // G or ALL
	ModeTs  int64
	Payload map[string]Member // membership list
}

func (n Query) String() string {
	s := "========= Request to join ==========\n"
	s += "Type: " + n.Header + "\n"
	s += "Address: " + n.Addr + "\n"
	s += "Heartbeat Counter:" + strconv.Itoa(n.Hc) + "\n"
	return s
}

func (m Member) String() string {
	s := "========= Member ==========\n"
	s += "ID: " + m.ID + "\n"
	s += "Address: " + m.Addr + "\n"
	s += "Hc:" + strconv.Itoa(m.Hc) + "\n"
	s += "TimeStamp:" + strconv.FormatInt(m.Timestamp, 10) + "\n"
	s += "Mode:" + m.Mode + "\n"
	return s
}

// Request for file operations
type Request struct {
	Header   string // request type: READ, WRITE, FILELIST, MASTER, PUT, GET
	FileName string
	FileSize int64
	FileList []fileop.File
	Payload  string // some indicators
}

// Response for file operations
type Response struct {
	Header   string // response type: SUCCESS, ERROR, PUT, GET
	FileSize int64
	Resp     []string
}

// MapReduce

var MRMaster = "172.22.156.132"

type MRQuery struct {
	Header  string // type: MAP, REDUCE
	Payload []string
}
