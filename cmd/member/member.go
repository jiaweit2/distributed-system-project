package main

import (
	"bufio"
	"cs425-mp1/pkg/client"
	"cs425-mp1/pkg/message"
	"cs425-mp1/pkg/server"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

var currMember message.Member

// membership list
var members map[string]message.Member

// JOINED the joined state of client
var JOINED bool = true

var mutex sync.Mutex

var currIP string

func main() {
	address := flag.String("addr", "fa20-cs425-g40-01.cs.illinois.edu", "ip address of introducer")
	interval := flag.Int("interval", 1, "heartbeat interval in seconds")
	LossRate := flag.Int("loss", 0, "loss rate")
	netInterface := flag.String("interface", "eth0", "specify network interface")
	mode := flag.String("mode", "ALL", "specify gossip mode")
	replica := flag.Int("replica", 3, "number of file replications on SDFS")
	flag.Parse()

	// start file server
	currIP = getIPAddr(*netInterface)
	go server.ServeFile(currIP, *replica)
	time.Sleep(1 * time.Second)

	// server channel for incoming membership lists
	memChan := make(chan map[string]message.Member, 1024)

	// channel for failed members
	sdfsFailChan := make(chan string, 1024)
	go server.CheckReReplicate(sdfsFailChan)

	// create log file
	file, err := os.Create("logs.txt")
	if err != nil {
		log.Fatal(err)
	}
	mw := io.MultiWriter(os.Stdout, file)
	log.SetOutput(mw)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	defer file.Close()

	startTime := time.Now().Unix()
	var speed float64

	// send initial heartbeats to the introducer
	members := initMembers(*address, *mode)

	// IP address of the current machine
	currMember = members[currIP]
	for ip, mem := range members {
		if ip == currIP || !mem.IsAlive {
			delete(members, ip)
		}
	}

	// sending heartbeat periodically
	go client.SendHeartbeat(currMember, JOINED, members, *interval, *LossRate)

	// start listening for heartbeats
	go server.ServeHeartbeat(memChan, &JOINED, &currMember, &mutex)

	// start MR server
	go server.ServeMR(currIP, members, &mutex)

	mrFailChan := make(chan string, 1024)
	go server.CheckMRWorkers(mrFailChan, members, &mutex)

	// merge membership list
	go message.MergeMembers(memChan, members, currIP, &mutex)

	// check timed out entries in membership list
	var timeNow int64
	go func() {
		for {
			time.Sleep(1 * time.Second) // check membership list every second
			mutex.Lock()
			if !JOINED {
				mutex.Unlock()
				continue
			}
			mutex.Unlock()
			timeNow = time.Now().Unix()
			for ip, mem := range members {
				// don't check whether youself and the introducer is alive
				if ip != currIP && ip != "INTRODUCER" {
					if mem.IsAlive && timeNow-mem.Timestamp > message.TIMEOUT {
						mem.IsAlive = false
						members[ip] = mem
						sdfsFailChan <- ip
						mrFailChan <- ip
						message.Log("Member " + ip + " is marked as failed")
						if val, ok := server.PutMap[ip]; ok {
							if val == true {
								server.PutMap[ip] = false
								server.PutWg.Done()
							}
						}
					}
					if !mem.IsAlive && timeNow-mem.Timestamp > message.CLEANUP {
						delete(members, ip)
						message.Log("Member " + ip + " is deleted from membership list")
					}
				}
			}
		}
	}()

	// Main Input Handler
	input := bufio.NewReader(os.Stdin)
	for {
		text, _ := input.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)
		args := strings.Split(text, " ")
		text = args[0]
		switch text {
		case "switch":
			mutex.Lock()
			if currMember.Mode == "ALL" {
				currMember.Mode = "G"
				fmt.Println("Switch to gossip mode")
			} else {
				currMember.Mode = "ALL"
				fmt.Println("Switch to all-to-all mode")
			}
			currMember.ModeTs = time.Now().Unix()
			mutex.Unlock()
		case "list":
			message.PrintMembers(members)
		case "id":
			fmt.Println(currMember.String())
		case "join":
			mutex.Lock()
			JOINED = true
			mutex.Unlock()
			log.Println(currIP + " joined")
		case "leave":
			mutex.Lock()
			JOINED = false
			mutex.Unlock()
			log.Println(currIP + " left")
		case "speed":
			speed = float64(client.Throughput) / float64((time.Now().Unix() - startTime))
			log.Println("Speed: " + fmt.Sprintf("%f", speed) + " Bps")
		case "exit":
			log.Println(currIP + " exited")
			return
		case "put":
			if len(args) < 3 {
				message.Log("please use get sdfsfilename localfilename")
				break
			}
			message.Log("putting file to SDFS..")
			localfilename, sdfsfilename := args[1], args[2]
			server.Put(localfilename, sdfsfilename, currIP)
		case "get":
			if len(args) < 3 {
				message.Log("please use get sdfsfilename localfilename")
				break
			}
			message.Log("getting file on SDFS..")
			sdfsfilename, localfilename := args[1], args[2]
			server.Get(localfilename, sdfsfilename)
		case "delete":
			if len(args) < 2 {
				message.Log("please use delete sdfsfilename")
				break
			}
			sdfsfilename := args[1]
			deleteRemote(sdfsfilename)
		case "store":
			server.PrintLocalFiles()
		case "ls":
			if len(args) < 2 {
				message.Log("please use delete sdfsfilename")
				break
			}
			sdfsfilename := args[1]
			ls(sdfsfilename)
			// default:
			//	fmt.Println("Please use the following command: list, id, switch, join, leave, speed, exit")
			//	fmt.Print("-> ")
		case "maple":
			if len(args) < 5 {
				message.Log("please use maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_file>")
				break
			}
			runMap(args[1], args[2], args[3], args[4])
		case "juice":
			if len(args) < 6 {
				message.Log("please use juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> <delete_input={0,1}>")
				break
			}
			runReduce(args[1], args[2], args[3], args[4], args[5])
		case "wipelocal":
			server.WipeAllLocal([]string{}, members)
		}
		fmt.Print("-> ")
	}

}

// Helper functions

func runMap(maple_exe string, num_maples string, sdfs_intermediate_filename_prefix string, sdfs_src_file string) {
	query := message.MRQuery{
		Header:  "MAP",
		Payload: []string{maple_exe, num_maples, sdfs_intermediate_filename_prefix, sdfs_src_file},
	}
	if currIP == message.MRMaster {
		// server.Put(sdfs_src_file, sdfs_src_file, currIP)
		go server.Map(query.Payload, members, &mutex)
	} else {
		client.MRSend(query, message.MRMaster)
	}
}

func runReduce(juice_exe string, num_juices string, sdfs_intermediate_filename_prefix string, sdfs_dest_filename string, delete_input string) {
	query := message.MRQuery{
		Header:  "REDUCE",
		Payload: []string{juice_exe, num_juices, sdfs_intermediate_filename_prefix, sdfs_dest_filename, delete_input},
	}
	if currIP == message.MRMaster {
		go server.Reduce(query.Payload, members, &mutex)
	} else {
		client.MRSend(query, message.MRMaster)
	}
}

func ls(sdfsfilename string) {
	if sdfsfilename == "" {
		fmt.Println("Missing params!")
		return
	}
	var addrs []string
	req := message.Request{Header: "GET", FileName: sdfsfilename}
	if server.Master != "" {
		conn, _ := client.Send(server.Master, req)
		res := client.Resp(conn)
		addrs = res.Resp
		conn.Close()
	} else {
		server.RequestChan <- server.Task{
			Conn: nil,
			Addr: "",
			Req:  req,
		}
		res := <-server.ResponseChan
		addrs = res.Resp
	}
	if len(addrs) == 0 {
		fmt.Println("File not found!")
	} else {
		fmt.Println("File is located at: ")
		fmt.Println(addrs)
	}
}

func deleteRemote(sdfsfilename string) {
	if sdfsfilename == "" {
		fmt.Println("Missing params!")
		return
	}
	req := message.Request{Header: "DEL", FileName: sdfsfilename}
	if server.Master != "" {
		conn, _ := client.Send(server.Master, req)
		conn.Close()
	} else {
		server.RequestChan <- server.Task{
			Conn: nil,
			Addr: "",
			Req:  req,
		}
	}
}

func initMembers(addr string, mode string) map[string]message.Member {
	members = make(map[string]message.Member)
	joinQuery := message.Query{
		Header:  "INTRO",
		Addr:    addr + ":8000",
		Hc:      1,
		Mode:    mode,
		Payload: members,
	}
	members, err := client.HeartBeat(joinQuery, 0)
	if err != nil {
		log.Fatal(err)
	}
	return members
}

func getIPAddr(netInterface string) string {
	eth, err := net.InterfaceByName(netInterface)
	if err != nil {
		log.Fatal(err)
	}
	laddrs, err := eth.Addrs()
	if err != nil {
		log.Fatal(err)
	}
	var laddr string
	for _, addr := range laddrs {
		switch v := addr.(type) {
		case *net.IPNet:
			if v.IP.To4().String() != "<nil>" {
				laddr = v.IP.To4().String()
			}
		}
	}
	return laddr
}

func elemIn(i int, array []int) bool {
	for _, elem := range array {
		if i == elem {
			return true
		}
	}
	return false
}
