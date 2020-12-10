package client

import (
	"bytes"
	"cs425-mp1/pkg/fileop"
	"cs425-mp1/pkg/message"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"log"
	"net"
	"os"
	"time"
)

// BUFFERSIZE size of receive buffer
var BUFFERSIZE int64 = 1024

// DIR local directory
var home, _ = os.UserHomeDir()

var DIR = home + "/local/"

// GetFile from remote directory to local directory
func GetFile(localfilename string, sdfsfilename string, addr string) error {
	req := message.Request{Header: "READ", FileName: sdfsfilename}
	conn, err := Send(addr, req)
	if err != nil {
		return err
	}
	defer conn.Close()
	res := Resp(conn)
	if res.Header == "ERROR" {
		log.Println("server error")
		return errors.New("server error")
	}
	fileSize := res.FileSize
	fileop.GetFile(conn, DIR, localfilename, fileSize, addr)
	return nil
}

// WriteFile to SDFS
func WriteFile(src string, localfilename string, sdfsfilename string, addr string) {
	dir := DIR
	if src == "cloud" {
		dir = home + "/cloud/"
	}
	file, err := os.Open(dir + localfilename)
	if err != nil {
		log.Println(err)
		return
	}
	fileInfo, err := file.Stat()
	if err != nil {
		log.Println(err)
		return
	}

	req := message.Request{Header: "WRITE", FileName: sdfsfilename, FileSize: fileInfo.Size()}
	conn, err := Send(addr, req)
	if err != nil {
		message.Log("dst node failed!")
		return
	}
	// message.Log("sending " + localfilename + " to " + addr)
	defer conn.Close()
	fileop.SendFile(conn, file)
}
func Send(addr string, req message.Request) (net.Conn, error) {
	conn, err := net.Dial("tcp", addr+":"+message.NODE_FILE_PORT)
	if err != nil {
		return nil, err
	}

	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	encoder.Encode(req)

	length := len(buffer.Bytes())
	header := make([]byte, 8)
	binary.LittleEndian.PutUint64(header, uint64(length))

	conn.Write(header)
	conn.Write(buffer.Bytes())
	return conn, nil
}

func Resp(conn net.Conn) message.Response {
	timeoutDuration := 5 * time.Second
	conn.SetReadDeadline(time.Now().Add(timeoutDuration))
	header := make([]byte, 8)
	_, err := conn.Read(header)
	if err != nil {
		return message.Response{}
	}
	n := int64(binary.LittleEndian.Uint64(header))
	buf := make([]byte, n)
	_, err = conn.Read(buf)
	if err != nil {
		return message.Response{}
	}
	var res message.Response
	err = gob.NewDecoder(bytes.NewBuffer(buf)).Decode(&res)
	if err != nil {
		log.Println(err)
	}
	return res
}
