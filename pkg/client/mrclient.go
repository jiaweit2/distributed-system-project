package client

import (
	"bytes"
	"cs425-mp1/pkg/message"
	"encoding/gob"
	"log"
	"net"
)

func MRSend(query message.MRQuery, addr string) {
	raddr, err := net.ResolveUDPAddr("udp", addr+":"+message.MR_PORT)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(query); err != nil {
		log.Fatal(err)
	}
	conn.Write(buffer.Bytes())
}
