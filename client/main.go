package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

type Client struct {
	addr string
	port int
	id   int
}

func (c *Client) run() {
	conn, id := c.connect()
	defer conn.Close()
	fmt.Printf("Create Client Success, Remote Addr = %s, Client ID = %d\n", conn.RemoteAddr().String(), id)
	fmt.Printf("COMMAND:\n\texit\n\tlock <LOCKNAME>\n\tunlock <LOCKNAME> \n\tcheck <LOCKNAME>\n")
	reader := bufio.NewReader(os.Stdin)
	for {
		getData := make([]byte, 512)
		var n int
		strBytes, _, _ := reader.ReadLine()
		msg := strings.Split(string(strBytes), " ")
		if msg[0] == "exit" {
			return
		}
		if len(msg) < 2 {
			continue
		}
		if msg[0] == "lock" {
			data := []byte(fmt.Sprintf("op::%d::TryLock %s %d", c.id, msg[1], c.id))
			conn.Write(data)
			n, _ = conn.Read(getData)
			msg = strings.Split(string(getData)[:n], "::")
			fmt.Println(msg[2])
			continue
		}
		if msg[0] == "unlock" {
			data := []byte(fmt.Sprintf("op::%d::TryUnLock %s %d", c.id, msg[1], c.id))
			conn.Write(data)
			n, _ = conn.Read(getData)
			msg = strings.Split(string(getData)[:n], "::")
			fmt.Println(msg[2])
			continue
		}
		if msg[0] == "check" {
			data := []byte(fmt.Sprintf("op::%d::Check %s", c.id, msg[1]))
			conn.Write(data)
			data = make([]byte, 512)
			n, _ = conn.Read(getData)
			msg = strings.Split(string(getData)[:n], "::")
			fmt.Println(msg[2])
			continue
		}

	}
}

func (c *Client) connect() (*net.TCPConn, int) {
	fmt.Printf("Start connect to server:\t%s:%d\n", c.addr, c.port)
	remoteAddr := net.TCPAddr{
		IP:   net.ParseIP(c.addr),
		Port: c.port,
	}
	conn, err := net.DialTCP("tcp4", nil, &remoteAddr)
	if err != nil {
		conn.Close()
		log.Fatal(err)
	}
	fmt.Println("Connect Success")
	fmt.Println("Start to Create New Client")
	var n int
	_, err = conn.Write([]byte(fmt.Sprintf("op::%d::NewClient", c.id)))
	if err != nil {
		fmt.Println("Create New Client Failed")
		log.Fatal(err)
	}
	data := make([]byte, 512)
	n, _ = conn.Read(data)
	msg := strings.Split(string(data)[:n], "::")
	if strings.HasPrefix(msg[2], "ClientId") {
		msg = strings.Split(msg[2], " ")
		c.id, _ = strconv.Atoi(msg[1])
		if c.id == 0 {
			log.Fatal("Get Client ID Failed")
		}
	}
	return conn, c.id
}

func main() {
	host := flag.String("host", "127.0.0.1", "host ip to connect")
	port := flag.Int("port", 9000, "port ip to connect")
	flag.Parse()
	client := Client{addr: *host, port: *port}
	client.run()
}
