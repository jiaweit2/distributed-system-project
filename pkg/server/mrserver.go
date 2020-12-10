package server

import (
	"bufio"
	"bytes"
	"cs425-mp1/pkg/client"
	"cs425-mp1/pkg/fileop"
	"cs425-mp1/pkg/message"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

var HOME, _ = os.UserHomeDir()
var currIP string
var startTime time.Time
var workers map[string][]string
var keySet map[string]bool // a set of keys produced by map workers
var willDeleteTempFiles bool
var MrWg, JuiceWg sync.WaitGroup
var mapleExe, intermediatePrefix string

type mrTask struct {
	Addr string
	Req  message.MRQuery
}

var mrTaskChan = make(chan mrTask, 50)

func ServeMR(ip string, members map[string]message.Member, mutex *sync.Mutex) {
	currIP = ip
	pc, err := net.ListenPacket("udp", ":"+message.MR_PORT)
	if err != nil {
		message.Log("MRServer Error")
		log.Fatal(err)
	}
	defer pc.Close()
	buffer := make([]byte, 2048)

	go handleQueuedTask(members, mutex)
	for {
		n, addr, err := pc.ReadFrom(buffer)
		if err != nil {
			message.Log("Connection has error: " + err.Error())
			continue
		}
		var query message.MRQuery
		if err := gob.NewDecoder(bytes.NewReader(buffer[:n])).Decode(&query); err != nil {
			message.Log("MRServer Error")
			log.Fatal(err)
		}
		address := strings.Split(addr.String(), ":")[0]
		if query.Header == "MAP" || query.Header == "REDUCE" {
			message.Log(query.Header)
			mrTaskChan <- mrTask{
				Addr: address,
				Req:  query,
			}
		} else {
			go handleMRQuery(mrTask{
				Addr: address,
				Req:  query,
			}, members, mutex)
		}

	}
}

func handleQueuedTask(members map[string]message.Member, mutex *sync.Mutex) {

	for {
		task := <-mrTaskChan
		MrWg.Wait()
		go handleMRQuery(task, members, mutex)
	}
}

func handleMRQuery(task mrTask, members map[string]message.Member, mutex *sync.Mutex) {
	query := task.Req
	address := task.Addr
	switch query.Header {
	case "MAP":
		if currIP == message.MRMaster {
			go Map(query.Payload, members, mutex)
		} else {
			go handleMapleTask(query.Payload)
		}
	case "REDUCE":
		if currIP == message.MRMaster {
			go Reduce(query.Payload, members, mutex)
		} else {
			go handleJuiceTask(query.Payload)
		}
	case "MAP-FIN":
		for _, keyFile := range query.Payload {
			keySet[keyFile] = true
		}

		delete(workers, address)
		message.Log("FIN!")
		MrWg.Done()
	case "REDUCE-FIN":
		delete(workers, address)
		MrWg.Done()
	case "WIPELOCAL":
		fileop.WipeLocal(query.Payload)
	}
}

func handleMapleTask(payload []string) {
	maple_exe, sdfs_src_file, sdfs_intermediate_filename_prefix := payload[0], payload[1], payload[2]
	message.Log("Maple worker: " + maple_exe + " " + sdfs_src_file)
	Get(sdfs_src_file, sdfs_src_file)

	cmd := &exec.Cmd{
		Path: "./apps/" + maple_exe,
		Args: []string{"./" + maple_exe, home + "/local/" + sdfs_src_file, sdfs_intermediate_filename_prefix},
	}
	var keyFiles []string

	if output, err := cmd.Output(); err != nil {
		log.Fatal(err)
	} else {
		message.Log(string(output))
		keyFiles = strings.Split(string(output), "\n")
		keyFiles = keyFiles[:len(keyFiles)-1]
		for _, keyFile := range keyFiles {
			Put(keyFile, keyFile, currIP)
		}
	}

	query := message.MRQuery{
		Header:  "MAP-FIN",
		Payload: keyFiles,
	}
	client.MRSend(query, message.MRMaster)
}

func handleJuiceTask(payload []string) {
	juice_exe, sdfs_intermediate_filename_prefix, sdfs_dest_filename := payload[0], payload[1], payload[2]
	message.Log(juice_exe)
	message.Log(sdfs_intermediate_filename_prefix)
	for i := 3; i < len(payload); i++ {
		intermediate_file := payload[i]
		JuiceWg.Add(1)
		go func() {
			Get(intermediate_file, intermediate_file)
			cmd := &exec.Cmd{
				Path: "./apps/" + juice_exe,
				Args: []string{"./" + juice_exe, home + "/local/" + intermediate_file, sdfs_dest_filename},
			}
			if _, err := cmd.Output(); err != nil {
				log.Fatal(err)
			}
			Put(sdfs_dest_filename, sdfs_dest_filename, currIP)
			JuiceWg.Done()
		}()
	}
	JuiceWg.Wait()
	query := message.MRQuery{
		Header: "REDUCE-FIN",
	}
	client.MRSend(query, message.MRMaster)
}

func Map(payload []string, members map[string]message.Member, mutex *sync.Mutex) {
	startTime = time.Now()

	keySet = make(map[string]bool)
	// Partition the input file and send to other nodes for maple_exe to run
	maple_exe, num_maples_str, sdfs_intermediate_filename_prefix, sdfs_src_file := payload[0], payload[1], payload[2], payload[3]
	num_maples, err := strconv.Atoi(num_maples_str)
	if err != nil || num_maples <= 0 {
		message.Log("must have a positive number of workers!")
	}
	mapleExe = maple_exe
	intermediatePrefix = sdfs_intermediate_filename_prefix
	// Store partitioned files in Master cloud/
	MrWg.Add(1)
	err = partition(num_maples, sdfs_src_file)

	if err != nil {
		MrWg.Done()
		return
	}
	// Distribute files to SDFS

	distribute(num_maples, sdfs_src_file)

	// assign maple workers
	workers = make(map[string][]string)
	mutex.Lock()
	cnt := 0

	message.Log("workers: " + strconv.Itoa(len(members)-1))
	if len(members)-1 < num_maples {
		MrWg.Done()
		message.Log("Not enough healthy workers!")
		return
	}
	for cnt < num_maples {
		for addr := range members {
			if addr == "INTRODUCER" || addr == currIP || !members[addr].IsAlive {
				continue
			}

			query := message.MRQuery{
				Header:  "MAP",
				Payload: []string{maple_exe, sdfs_src_file + "_" + strconv.Itoa(cnt), sdfs_intermediate_filename_prefix},
			}
			client.MRSend(query, addr)
			workers[addr] = []string{maple_exe, sdfs_src_file + "_" + strconv.Itoa(cnt), sdfs_intermediate_filename_prefix}
			message.Log("Maple task is assigned to: " + addr)
			fmt.Println(workers[addr])
			MrWg.Add(1)
			cnt++
			if cnt == num_maples {
				break
			}

		}
	}
	mutex.Unlock()
	MrWg.Done()
	MrWg.Wait()
	message.Log("Maple is finished and it took " + time.Now().Sub(startTime).String())
}

func Reduce(payload []string, members map[string]message.Member, mutex *sync.Mutex) {
	juice_exe, num_juices_str, sdfs_intermediate_filename_prefix, sdfs_dest_filename, delete_input := payload[0], payload[1], payload[2], payload[3], payload[4]
	num_juices, err := strconv.Atoi(num_juices_str)
	if err != nil || num_juices <= 0 {
		message.Log("must have a positive number of workers!")
	}
	if delete_input == "1" {
		willDeleteTempFiles = true
	} else {
		willDeleteTempFiles = false
	}

	MrWg.Add(1)
	// transform keysMap to a list
	var keys []string
	for k := range keySet {
		keys = append(keys, k)
	}
	if len(keys) < num_juices {
		num_juices = len(keys)
	}
	portionSize := len(keys) / num_juices

	// assign juice workers
	workers = make(map[string][]string)
	cnt := 0
	mutex.Lock()
	for addr := range members {
		if addr == "INTRODUCER" || addr == currIP {
			continue
		}
		end := cnt + portionSize
		if num_juices == 1 {
			end = len(keys)
		}

		payload := []string{juice_exe, sdfs_intermediate_filename_prefix, sdfs_dest_filename}
		for i := cnt; i < end; i++ {
			payload = append(payload, keys[i])
		}
		query := message.MRQuery{
			Header:  "REDUCE",
			Payload: payload,
		}
		client.MRSend(query, addr)
		workers[addr] = payload
		message.Log("Reduce task is assigned to: " + addr)
		MrWg.Add(1)
		num_juices--
		cnt = end
		if num_juices == 0 {
			break
		}
	}
	mutex.Unlock()
	MrWg.Done()

	MrWg.Wait()
	message.Log("MR is finished and it took " + time.Now().Sub(startTime).String())
	if willDeleteTempFiles {
		WipeAllLocal([]string{"FinalOutput"}, members)
	}

}

func WipeAllLocal(exceptions []string, members map[string]message.Member) {
	fileop.WipeLocal(exceptions)
	for addr := range members {
		if addr == "INTRODUCER" || addr == currIP {
			continue
		}
		query := message.MRQuery{
			Header:  "WIPELOCAL",
			Payload: exceptions,
		}
		client.MRSend(query, addr)
	}
}

func partition(num_maples int, sdfs_src_file string) error {
	var partitionedFiles []*os.File
	if !fileop.FileExists(sdfs_src_file) {
		Get(sdfs_src_file, sdfs_src_file)
	}
	for i := 0; i < num_maples; i++ {
		dir := home + "/local/" + sdfs_src_file + "_" + strconv.Itoa(i)
		f, _ := os.Create(dir)
		partitionedFiles = append(partitionedFiles, f)
		defer f.Close()
	}
	file, err := os.Open(home + "/local/" + sdfs_src_file)
	if err != nil {
		message.Log(sdfs_src_file + "does not exist!")
		return err
	}
	defer file.Close()
	line := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		i := line % num_maples
		line++
		partitionedFiles[i].WriteString(scanner.Text() + "\n")
	}
	message.Log("finished partition")
	return nil
}

func distribute(num_maples int, sdfs_src_file string) {
	for i := 0; i < num_maples; i++ {
		localfilename := sdfs_src_file + "_" + strconv.Itoa(i)
		sdfsfilename := localfilename
		Put(localfilename, sdfsfilename, currIP)
	}
}

func CheckMRWorkers(failChan <-chan string, members map[string]message.Member, mutex *sync.Mutex) {
	for {
		failedWorker := <-failChan
		if currIP == message.MRMaster {
			if task, ok := workers[failedWorker]; ok {
				// re-asign juice worker
				mutex.Lock()
				for addr := range members {
					if addr == "INTRODUCER" || addr == currIP || !members[addr].IsAlive {
						continue
					}
					workers[addr] = task
					fmt.Println(task)
					header := "REDUCE"
					if len(task) == 3 {
						header = "MAP"
					}
					query := message.MRQuery{
						Header:  header,
						Payload: task,
					}
					client.MRSend(query, addr)
					message.Log(header + " task is assigned to: " + addr)
					break
				}
				mutex.Unlock()
				delete(workers, failedWorker)
			}
		}
	}
}

func sendMrWait(payload string) {
	req := message.Request{Header: "MR", Payload: payload}
	conn, err := client.Send(Master, req)
	if err == nil {
		conn.Close()
	}
}
