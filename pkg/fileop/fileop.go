package fileop

import (
	"io"
	"log"
	"net"
	"os"
	"strings"
)

// BUFFERSIZE size of buffer
var BUFFERSIZE int64 = 1024

// Home dir
var home, _ = os.UserHomeDir()

// File struct
type File struct {
	FileName string
	FileSize int64
}

// SendFile send file to destination
func SendFile(conn net.Conn, file *os.File) {
	defer file.Close()
	buffer := make([]byte, BUFFERSIZE)
	for {
		_, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		conn.Write(buffer)
	}
}

// GetFile get file from connection
func GetFile(conn net.Conn, dir string, fileName string, fileSize int64, addr string) {

	var receivedFile *os.File
	var err error
	if strings.Contains(dir, "/local/") {
		receivedFile, err = os.Create(dir + fileName)
	} else {
		receivedFile, err = os.OpenFile(dir+fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	}

	if err != nil {
		log.Fatal(err)
	}
	var receivedBytes int64

	for {
		if (fileSize - receivedBytes) < BUFFERSIZE {
			io.CopyN(receivedFile, conn, (fileSize - receivedBytes))
			conn.Read(make([]byte, receivedBytes+BUFFERSIZE-fileSize))
			break
		}
		io.CopyN(receivedFile, conn, BUFFERSIZE)
		receivedBytes += BUFFERSIZE
	}
	receivedFile.Close()

}

// Remove local sdfsfile
func DeleteFile(fileName string) {
	e := os.Remove(fileName)
	if e != nil {
		log.Fatal(e)
	}
}

// Check if local file exists
func FileExists(filename string) bool {
	_, err := os.Stat(home + "/local/" + filename)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

// Wipe all local sdfsfiles
func Wipe() {
	_ = os.RemoveAll(home + "/cloud/")
	_ = os.Mkdir(home+"/cloud/", 0755)
	WipeLocal([]string{})
}

func WipeLocal(exceptions []string) {
	exceptions = append(exceptions, "CondorcetVotes")
	exceptions = append(exceptions, "Tree")
	var toRecover []string
	for _, filename := range exceptions {
		if FileExists(filename) {
			_ = os.Rename(home+"/local/"+filename, home+"/"+filename)
			toRecover = append(toRecover, filename)
		}
	}

	err := os.RemoveAll(home + "/local/")
	if err != nil {
		log.Fatal(err)
	}
	_ = os.Mkdir(home+"/local/", 0755)

	for _, filename := range toRecover {
		_ = os.Rename(home+"/"+filename, home+"/local/"+filename)
	}
}
