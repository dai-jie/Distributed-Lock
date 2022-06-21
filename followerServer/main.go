package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
)

type Follower struct {
	leaderAddr string
	leaderPort int
	selfAddr   string
	selfPort   int
	clients    map[int]*net.TCPConn
	lockMap    map[string]int
	id         int
}

func (f *Follower) connectWithLeader() (*net.TCPConn, int) {
	fmt.Printf("start to connect to leader server: %s:%d\n", f.leaderAddr, f.leaderPort)
	remoteAddr := net.TCPAddr{
		IP:   net.ParseIP(f.leaderAddr),
		Port: f.leaderPort,
	}
	conn, err := net.DialTCP("tcp4", nil, &remoteAddr)
	if err != nil {
		conn.Close()
		log.Fatal(err)
	}
	var n int
	_, err = conn.Write([]byte(fmt.Sprintf("op::%d::NewServer", f.id)))
	if err != nil {
		fmt.Println("Create New Client Failed")
		log.Fatal(err)
	}
	data := make([]byte, 512)
	n, _ = conn.Read(data)
	msg := strings.Split(string(data)[:n], "::")
	if strings.HasPrefix(msg[2], "ServerId") {
		msg = strings.Split(msg[2], " ")
		f.id, _ = strconv.Atoi(msg[1])
	} else {
		log.Fatal("Unknown message from leader server")
	}
	return conn, f.id
}

func (f *Follower) run() {
	leaderConn, id := f.connectWithLeader()
	defer leaderConn.Close()
	fmt.Printf("Create Follower Server Success, Remote Addr = %s, Server ID = %d\n", leaderConn.RemoteAddr().String(), id)
	go f.fromLeader(leaderConn)
	address := net.TCPAddr{
		IP:   net.ParseIP(f.selfAddr),
		Port: f.selfPort,
	}
	listener, err := net.ListenTCP("tcp4", &address)
	if err != nil {
		log.Fatal(err)
	}
	for {
		clientConn, err := listener.AcceptTCP()
		if err != nil {
			log.Fatal(err)
		}
		go f.clientToLeader(leaderConn, clientConn)
	}

}

func (f *Follower) fromLeader(conn *net.TCPConn) {
	defer conn.Close()
	getData := make([]byte, 512)
	var n int
	for {
		n, _ = conn.Read(getData)
		dataStr := string(getData)[:n]
		if dataStr == "" {
			continue
		}
		msg := strings.Split(dataStr, "::")

		if strings.HasPrefix(msg[2], "UpdateLock") {
			msg = strings.Split(msg[2], " ")
			id, _ := strconv.Atoi(msg[2])
			f.lockMap[msg[1]] = id
			continue
		}
		if strings.HasPrefix(msg[2], "DeleteLock") {
			msg = strings.Split(msg[2], " ")
			delete(f.lockMap, msg[1])
			continue
		}
		if strings.HasPrefix(msg[2], "There") || strings.HasPrefix(msg[2], "PreemptLock") ||
			strings.HasPrefix(msg[2], "ReleaseLock") {
			client, _ := strconv.Atoi(msg[1])
			f.clients[client].Write([]byte(dataStr))
			continue
		}

	}

}

func (f *Follower) clientToLeader(leader *net.TCPConn, client *net.TCPConn) {
	defer client.Close()
	remoteAddr := net.TCPAddr{
		IP:   net.ParseIP(f.leaderAddr),
		Port: f.leaderPort,
	}
	leaderCreateClient, err := net.DialTCP("tcp4", nil, &remoteAddr)
	defer leaderCreateClient.Close()
	if err != nil {
		log.Fatal(err)
	}
	for {
		reader := bufio.NewReader(client)
		data := make([]byte, 512)
		n, err := reader.Read(data[:])
		if err != nil {
			log.Println(err)
			return
		}
		dataStr := string(data[:n])
		fmt.Printf("receive data : %s\n", dataStr)
		if dataStr == "" {
			continue
		}
		msg := strings.Split(dataStr, "::")
		if len(msg) != 3 {
			fmt.Println("unknown message")
			continue
		}
		if msg[2] == "NewClient" {
			_, err := leaderCreateClient.Write([]byte(dataStr))
			if err != nil {
				log.Fatal(err)
			}
			getData := make([]byte, 512)
			m, _ := leaderCreateClient.Read(getData)
			getDataStr := string(getData)[:m]
			msgFromLeader := strings.Split(getDataStr, "::")
			if strings.HasPrefix(msgFromLeader[2], "ClientId") {
				msg = strings.Split(msgFromLeader[2], " ")
				id, _ := strconv.Atoi(msg[1])
				f.clients[id] = client
				if id == 0 {
					log.Println("Follower Get Client ID Failed")
				}
				fmt.Println(string(getData)[:m])
				client.Write([]byte(getDataStr))
				continue
			}
			continue
		}

		if strings.HasPrefix(msg[2], "Check") {
			sender, _ := strconv.Atoi(msg[1])
			msg = strings.Split(msg[2], " ")
			if id, ok := f.lockMap[msg[1]]; ok {
				client.Write([]byte(fmt.Sprintf("msg::%d::There is a lock named %s owned by %d", sender, msg[1], id)))
			} else {
				client.Write([]byte(fmt.Sprintf("msg::%d::There doesn't exist lock named %s", sender, msg[1])))
			}
			continue
		}

		if strings.HasPrefix(msg[2], "TryLock") {
			sender, _ := strconv.Atoi(msg[1])
			msg = strings.Split(msg[2], " ")
			res, tmpMsg := f.tryLock(msg[1], msg[2], leader, &dataStr)
			if !res {
				client.Write([]byte(fmt.Sprintf("msg::%d::PreemptLock Failed, LOCK EXISTS ", sender) + tmpMsg))
			}
			continue
		}

		if strings.HasPrefix(msg[2], "TryUnLock") {
			sender, _ := strconv.Atoi(msg[1])
			msg = strings.Split(msg[2], " ")
			res, tmpMsg := f.tryUnLock(msg[1], msg[2], leader, &dataStr)
			if !res {
				client.Write([]byte(fmt.Sprintf("msg::%d::ReleaseLock Failed ", sender) + tmpMsg))
			}
			continue
		}
	}
}

func (f *Follower) tryLock(keyName string, id string, leader *net.TCPConn, dataStr *string) (bool, string) {
	if _, ok := f.lockMap[keyName]; ok {
		return false, fmt.Sprintf("{LOCKNAME: %s OWNER: %d}", keyName, f.lockMap[keyName])
	} else {
		leader.Write([]byte(*dataStr))
		return true, ""
	}
}

func (f *Follower) tryUnLock(name string, clientId string, leader *net.TCPConn, dataStr *string) (bool, string) {
	if _, ok := f.lockMap[name]; !ok {
		return false, "ERROR: LOCK DOESN'T EXISTS"
	} else {
		id, _ := strconv.Atoi(clientId)
		if id != f.lockMap[name] {
			return false, fmt.Sprintf("CAN'T ACCESS THIS LOCK OWNED BY CLINET %d", f.lockMap[name])
		} else {
			leader.Write([]byte(*dataStr))
			return true, ""
		}
	}
}

func main() {
	port := flag.Int("port", 9001, "self port")
	leader := flag.String("leader", "127.0.0.1:9000", "leader")
	flag.Parse()
	addr := strings.Split(*leader, ":")
	leaderPort, _ := strconv.Atoi(addr[1])
	fmt.Println(addr[0], leaderPort)
	follower := Follower{
		leaderAddr: addr[0],
		leaderPort: leaderPort,
		selfAddr:   "127.0.0.1",
		selfPort:   *port,
		clients:    make(map[int]*net.TCPConn),
		lockMap:    make(map[string]int),
	}
	follower.run()
}
