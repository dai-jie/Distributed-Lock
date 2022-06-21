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

type Leader struct {
	port      int
	addr      string
	clients   map[int]*net.TCPConn
	followers map[int]*net.TCPConn
	lockMap   map[string]int
}

func (l *Leader) newClient() int {
	return len(l.clients) + 1
}

func (l *Leader) run() {
	address := net.TCPAddr{
		IP:   net.ParseIP(l.addr), // 把字符串IP地址转换为net.IP类型
		Port: l.port,
	}

	listener, err := net.ListenTCP("tcp4", &address) // 创建TCP4服务器端监听器
	if err != nil {
		log.Fatal(err) // Println + os.Exit(1)
	}
	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Println(err) // 错误直接退出
		}
		go l.response(conn)
	}
}

func (l *Leader) response(conn *net.TCPConn) {
	defer conn.Close()
	for {
		reader := bufio.NewReader(conn)
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
			clientId := l.newClient()
			l.clients[clientId] = conn
			_, err2 := conn.Write([]byte(fmt.Sprintf("msg::%d::ClientId %d", clientId, clientId)))
			if err2 != nil {
				log.Println(err2)
			}
			fmt.Printf("a new client: id = %d, addr=%s\n", clientId, conn.RemoteAddr().String())
			continue
		}

		if msg[2] == "NewServer" {
			serverId := l.newServer()
			l.followers[serverId] = conn
			_, err := conn.Write([]byte(fmt.Sprintf("msg::%d::ServerId %d", serverId, serverId)))
			if err != nil {
				log.Println(err)
				return
			}
			fmt.Printf("a new follower: id = %d\n", serverId)
			continue
		}

		if strings.HasPrefix(msg[2], "Check") {
			sender, _ := strconv.Atoi(msg[1])
			msg = strings.Split(msg[2], " ")
			if id, ok := l.lockMap[msg[1]]; ok {
				conn.Write([]byte(fmt.Sprintf("msg::%d::There is a lock named %s owned by %d", sender, msg[1], id)))
			} else {
				conn.Write([]byte(fmt.Sprintf("msg::%d::There doesn't exist lock named %s", sender, msg[1])))
			}
			continue
		}

		if strings.HasPrefix(msg[2], "TryLock") {
			sender, _ := strconv.Atoi(msg[1])
			msg = strings.Split(msg[2], " ")
			res, tmpMsg := l.tryLock(msg[1], msg[2])
			//id, _ := strconv.Atoi(msg[2])
			if !res {
				conn.Write([]byte(fmt.Sprintf("msg::%d::PreemptLock Failed, LOCK EXISTS ", sender) + tmpMsg))
			} else {
				conn.Write([]byte(fmt.Sprintf("msg::%d::PreemptLock Success", sender)))
			}
			continue
		}

		if strings.HasPrefix(msg[2], "TryUnLock") {
			sender, _ := strconv.Atoi(msg[1])
			msg = strings.Split(msg[2], " ")
			res, tmpMsg := l.tryUnLock(msg[1], msg[2])
			//id, _ := strconv.Atoi(msg[2])
			if !res {
				conn.Write([]byte(fmt.Sprintf("msg::%d::ReleaseLock Failed ", sender) + tmpMsg))
			} else {
				conn.Write([]byte(fmt.Sprintf("msg::%d::ReleaseLock Success", sender)))
			}
			continue
		}

	}
}

func (l *Leader) newServer() int {
	return len(l.followers) + 1
}

func (l *Leader) tryLock(keyName string, id string) (bool, string) {
	if _, ok := l.lockMap[keyName]; ok {
		return false, fmt.Sprintf("{LOCKNAME: %s OWNER: %d}", keyName, l.lockMap[keyName])
	} else {
		l.lockMap[keyName], _ = strconv.Atoi(id)
		l.broadcast(keyName, id, "add")
		return true, ""
	}
}

func (l *Leader) broadcast(name string, id string, update string) {
	if update == "add" {
		for _, conn := range l.followers {
			conn.Write([]byte(fmt.Sprintf("op::0::UpdateLock %s %s", name, id)))
		}
	} else if update == "delete" {
		for _, conn := range l.followers {
			conn.Write([]byte(fmt.Sprintf("op::0::DeleteLock %s %s", name, id)))
		}
	}
	return
}

func (l *Leader) tryUnLock(name string, clientId string) (bool, string) {
	if _, ok := l.lockMap[name]; !ok {
		return false, "ERROR: LOCK DOESN'T EXISTS"
	} else {
		id, _ := strconv.Atoi(clientId)
		if id != l.lockMap[name] {
			return false, fmt.Sprintf("CAN'T ACCESS THIS LOCK OWNED BY CLINET %d", l.lockMap[name])
		} else {
			delete(l.lockMap, name)
			l.broadcast(name, clientId, "delete")
			return true, ""
		}
	}
}

func main() {
	host := flag.String("host", "127.0.0.1", "host")
	port := flag.Int("port", 9000, "port")
	flag.Parse()
	leader := Leader{
		addr:      *host,
		port:      *port,
		lockMap:   make(map[string]int),
		followers: make(map[int]*net.TCPConn),
		clients:   make(map[int]*net.TCPConn),
	}
	leader.run()
}
