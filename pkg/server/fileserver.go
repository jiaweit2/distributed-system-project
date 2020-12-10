package server

import (
	"bytes"
	"cs425-mp1/pkg/client"
	"cs425-mp1/pkg/fileop"
	"cs425-mp1/pkg/message"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

var home, _ = os.UserHomeDir()
var ip string

// DIR file directory
var DIR = home + "/cloud/"

// Local file storage metadata
// first elem is the total local file size
var LocalFiles = []fileop.File{fileop.File{FileSize: 0}}

// Master info
var Master = "DEFAULT"
var Metas = make(map[string][]fileop.File)

var replica int

// Task queue
type Task struct {
	Conn net.Conn
	Addr string
	Req  message.Request
}

var RequestChan = make(chan Task, 30)
var ResponseChan = make(chan message.Response, 30)
var PutWg sync.WaitGroup
var fileMetaMutex sync.Mutex
var PutMap map[string]bool

// ServeFile handles getFile requests
func ServeFile(currIP string, rep int) {
	fileop.Wipe()
	server, err := net.Listen("tcp", ":"+message.NODE_FILE_PORT)
	replica = rep
	if err != nil {
		log.Fatal(err)
	}
	defer server.Close()

	ip = currIP

	go handleQueuedRequest()
	for {
		conn, err := server.Accept()
		if err != nil {
			log.Println(err)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	header := make([]byte, 8)
	conn.Read(header)
	n := int64(binary.LittleEndian.Uint64(header))
	buf := make([]byte, n)
	conn.Read(buf)
	var req message.Request
	err := gob.NewDecoder(bytes.NewBuffer(buf)).Decode(&req)
	if err != nil {
		log.Println(err)
		conn.Close()
		return
	}
	addr := strings.Split(conn.RemoteAddr().String(), ":")[0]
	// message.Log("FileServer receives " + req.Header + " request")

	if req.Header == "GET" || req.Header == "PUT" || req.Header == "DEL" {
		// add to task queue
		RequestChan <- Task{
			Conn: conn,
			Addr: addr,
			Req:  req,
		}
	} else {
		go handleRequest(Task{
			Conn: conn,
			Addr: addr,
			Req:  req,
		})
	}
}

func handleQueuedRequest() {
	for {
		task := <-RequestChan
		PutWg.Wait()
		PutMap = map[string]bool{}
		message.Log("filelist up to date")
		handleRequest(task)
	}
}

func handleRequest(task Task) {

	conn := task.Conn
	addr := task.Addr
	req := task.Req

	// only these operation will be blocked
	//if req.Header == "GET" || req.Header == "PUT" || req.Header == "DEL" {
	//	Wg.Wait()
	//	message.Log("previous put-ack received")
	//	PutWg.Wait()
	//	message.Log("filelist up to date")
	// }

	switch req.Header {
	case "READ":
		file, err := os.Open(DIR + req.FileName)
		if err != nil {
			log.Println(err)
			conn.Close()
			// continue
		}
		fileInfo, err := file.Stat()
		if err != nil {
			log.Println(err)
			conn.Close()
			// continue
		}
		res := message.Response{Header: "SUCCESS", FileSize: fileInfo.Size()}
		sendResp(conn, res)
		fileop.SendFile(conn, file)
	case "WRITE":
		var rewrite = false
		for i, file := range LocalFiles {
			// find if the file already exists
			if file.FileName == req.FileName {
				LocalFiles[0].FileSize += req.FileSize - file.FileSize
				LocalFiles[i].FileSize = req.FileSize
				rewrite = true
				break
			}
		}
		if !rewrite {
			LocalFiles[0].FileSize += req.FileSize
			LocalFiles = append(LocalFiles, fileop.File{FileName: req.FileName, FileSize: req.FileSize})
		}

		fileop.GetFile(conn, DIR, req.FileName, req.FileSize, addr)
		// message.Log(currIP + " received: " + req.FileName)
		if Master != "" {
			sendFileMeta("WRITE")
		} else {
			fileMetaMutex.Lock()
			Metas[ip] = LocalFiles
			fileMetaMutex.Unlock()
			// message.Log("Receive Filelist from Master")
			PutWg.Done()
		}

	case "DELETE":
		fileop.DeleteFile(DIR + req.FileName)
		newLocalFiles := []fileop.File{fileop.File{FileSize: 0}}
		for i, file := range LocalFiles {
			if i > 0 && file.FileName != req.FileName {
				newLocalFiles = append(newLocalFiles, file)
				newLocalFiles[0].FileSize += file.FileSize
			}
		}
		LocalFiles = newLocalFiles
		if Master != "" {
			sendFileMeta("DELETE")
		} else {
			fileMetaMutex.Lock()
			Metas[ip] = LocalFiles
			fileMetaMutex.Unlock()
		}
	case "REPLICATE":
		// message.Log("Replicating file: " + req.FileName)
		client.WriteFile("cloud", req.FileName, req.FileName, req.Payload)

	// Master ops
	case "GET":
		addrsHasFile := Search(req.FileName)
		res := message.Response{Header: "GET", Resp: addrsHasFile}
		if conn != nil {
			sendResp(conn, res)
		} else {
			ResponseChan <- res
		}
		message.Log("Get file: " + req.FileName)
	case "PUT":
		addrsToWrite := Search(req.FileName)
		if len(addrsToWrite) == 0 {
			message.Log("First write to file: " + req.FileName)
			addrsToWrite = DecideDest(req.Payload)
		} else {
			message.Log("Append to file: " + req.FileName)
		}
		fmt.Println(addrsToWrite)
		res := message.Response{Header: "PUT", Resp: addrsToWrite}
		if conn != nil {
			sendResp(conn, res)
		} else {
			ResponseChan <- res
		}
		for _, addr := range addrsToWrite {
			PutMap[addr] = true
		}
		PutWg.Add(len(addrsToWrite))
	case "DEL":
		DeleteRemote(req.FileName)
	case "FILELIST":
		fileMetaMutex.Lock()
		Metas[addr] = req.FileList
		fileMetaMutex.Unlock()
		// message.Log("Receive filelist from " + addr)
		// message.Log(req.Payload)
		if req.Payload == "WRITE" {
			PutMap[addr] = false
			PutWg.Done()
			// message.Log("DEC!")
		}
	case "MASTER":
		if req.Payload == ip && Master != "" {
			// I am the master now
			Master = ""
			fileMetaMutex.Lock()
			Metas[ip] = LocalFiles
			fileMetaMutex.Unlock()
			message.Log("Set to be the master")
			go iniReplicate()
		} else if req.Payload != ip {
			// Store Master info
			Master = req.Payload
			sendFileMeta("INITIAL")
		}
	}
	if conn != nil {
		conn.Close()
	}

}

// Helper functions

// iniReplicate checks and re-replicates file when a new master is selected
func iniReplicate() {
	if Master == "" {
		time.Sleep(time.Duration(2) * time.Second)
		fileSet := make(map[string]bool)
		for _, fileList := range Metas {
			for _, file := range fileList {
				if _, ok := fileSet[file.FileName]; !ok {
					fileSet[file.FileName] = true
					dst, src := checkFileReplica(file)
					if len(src) < replica && len(dst) > 0 {
						reReplicate(file.FileName, dst, src)
					}
				}
			}
		}
	}
}

/* checkFileReplica returns dst, src
** reFile: file to be checked
** src: a list of node that has the file,
** dst: a list of node that does not have the file
 */
func checkFileReplica(reFile fileop.File) ([]string, []string) {

	var dst []string
	var src []string
	var fileExists bool
	for addr, files := range Metas {
		fileExists = false
		for _, file := range files {
			if file.FileName == reFile.FileName {
				src = append(src, addr)
				fileExists = true
				break
			}
		}
		if fileExists == false {
			dst = append(dst, addr)
		}
	}

	return dst, src
}

// CheckReReplicate re-replicate files one a node fails
func CheckReReplicate(failChan <-chan string) {
	for {
		ip := <-failChan
		if Master == "" {
			fileList := Metas[ip]
			delete(Metas, ip)
			// re-replicate each file
			var dst []string
			var src []string
			for _, reFile := range fileList {
				dst, src = checkFileReplica(reFile)
				if len(dst) > 0 && len(src) < replica {
					reReplicate(reFile.FileName, dst, src)
				}
			}
		}
	}
}

func reReplicate(fileName string, dst []string, src []string) {
	if len(src) == 0 {
		message.Log(fileName + " is lost")
		return
	}
	if len(src) >= replica || len(dst) == 0 {
		return
	}
	rep := replica - len(src)
	if rep > len(dst) {
		rep = len(dst)
	}
	for i := 0; i < rep; i++ {
		req := message.Request{Header: "REPLICATE", FileName: fileName, Payload: dst[i]}
		PutWg.Add(1)
		for j := 0; j < len(src); j++ {
			conn, err := client.Send(src[j], req)
			if err == nil {
				conn.Close()
				break
			}
		}

	}

}

func Search(filename string) []string {
	var addrs []string
	for addr, files := range Metas {
		for _, file := range files {
			if file.FileName == filename {
				addrs = append(addrs, addr)
				break
			}
		}
	}
	return addrs
}

func DeleteRemote(filename string) {
	req := message.Request{Header: "DELETE", FileName: filename}
	for addr, files := range Metas {
		for _, file := range files {
			if file.FileName == filename {
				conn, _ := client.Send(addr, req)
				conn.Close()
			}
		}
	}
}

// for sorting purpose
type entry struct {
	val int64
	key string
}
type entries []entry

func (s entries) Len() int           { return len(s) }
func (s entries) Less(i, j int) bool { return s[i].val < s[j].val }
func (s entries) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func DecideDest(exclude string) []string {
	// find the number of replica servers with smallest disk usage
	var es entries
	for addr, files := range Metas {
		// not replicate to itself
		// if addr == exclude {
		// 	continue
		// }
		es = append(es, entry{val: files[0].FileSize, key: addr})
	}
	sort.Sort(es)
	var addrs []string
	for i, entry := range es {
		addrs = append(addrs, entry.key)
		if i == replica-1 {
			break
		}
	}
	return addrs
}

func sendResp(conn net.Conn, res message.Response) {
	var msgBuffer bytes.Buffer
	encoder := gob.NewEncoder(&msgBuffer)
	encoder.Encode(res)

	length := len(msgBuffer.Bytes())
	header := make([]byte, 8)
	binary.LittleEndian.PutUint64(header, uint64(length))

	conn.Write(header)
	conn.Write(msgBuffer.Bytes())
}

func sendFileMeta(payload string) {
	req := message.Request{Header: "FILELIST", FileList: LocalFiles, Payload: payload}
	conn, err := client.Send(Master, req)
	if err == nil {
		conn.Close()
	}
}

func PrintLocalFiles() {
	var s string
	for i, file := range LocalFiles {
		if i > 0 {
			s += file.FileName + "\t" + fmt.Sprintf("%v", file.FileSize) + "\n"
		}
	}
	fmt.Printf(s)
}

func GetDest(currIP string, sdfsfilename string) []string {
	var addrs []string
	req := message.Request{Header: "PUT", FileName: sdfsfilename, Payload: currIP}
	if Master != "" {
		conn, _ := client.Send(Master, req)
		res := client.Resp(conn)
		addrs = res.Resp
		conn.Close()
	} else {
		RequestChan <- Task{
			Conn: nil,
			Addr: "",
			Req:  req}
		res := <-ResponseChan
		addrs = res.Resp
	}
	return addrs
}

func Put(localfilename string, sdfsfilename string, currIP string) {
	message.Log("start put")
	if !fileop.FileExists(localfilename) {
		message.Log(localfilename + " does not exists!")
		return
	}
	if sdfsfilename == "" {
		message.Log("sdfsfilename is empty!")
		return
	}
	var addrs []string
	for i := 0; i < 2; i++ {

		req := message.Request{Header: "PUT", FileName: sdfsfilename, Payload: currIP}
		if Master != "" {
			conn, err := client.Send(Master, req)
			if err != nil {
				message.Log("File Master failed!")
				return
			}
			res := client.Resp(conn)
			addrs = res.Resp
			conn.Close()
		} else {
			RequestChan <- Task{
				Conn: nil,
				Addr: "",
				Req:  req,
			}
			res := <-ResponseChan
			addrs = res.Resp
		}
		if len(addrs) == 0 {
			fmt.Println("No addr to write")
			time.Sleep(3)
			fmt.Println("Refetch...")
		} else {
			break
		}
	}
	for _, addr := range addrs {
		client.WriteFile("local", localfilename, sdfsfilename, addr)
	}
	message.Log("File " + sdfsfilename + " is sent to: ")
	fmt.Println(addrs)
	fmt.Print("-> ")

}

func Get(localfilename string, sdfsfilename string) {
	if localfilename == "" || sdfsfilename == "" {
		message.Log("Missing params!")
		return
	}
	// return if file already exists
	// if fileop.FileExists(localfilename) {
	// 	message.Log("File already exists in local/")
	// 	return
	// }
	var addrs []string

	for i := 0; i < 2; i++ {
		req := message.Request{Header: "GET", FileName: sdfsfilename}
		if Master != "" {
			conn, _ := client.Send(Master, req)
			res := client.Resp(conn)
			addrs = res.Resp
			conn.Close()
		} else {
			RequestChan <- Task{
				Conn: nil,
				Addr: "",
				Req:  req,
			}
			res := <-ResponseChan
			addrs = res.Resp
		}

		if len(addrs) == 0 {
			fmt.Println("File doesn't exist!")
			time.Sleep(3)
			fmt.Println("Refetch...")
		} else {
			break
		}
	}

	// try to get file at first valid address
	for _, addr := range addrs {
		err := client.GetFile(localfilename, sdfsfilename, addr)
		if err == nil {
			message.Log("File " + localfilename + " is received! (in your local/)")
			break
		} else {
			message.Log("Get file from " + addr + " has failed. Trying the next one...")
		}
	}
}
