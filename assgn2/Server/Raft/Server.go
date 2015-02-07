package Raft

import (
	"log"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"fmt"
)

//For distinguishing of clients by the kvStore
var connLog = make(map[LogEntry]net.Conn)
var commitCh= make(chan LogEntry)

//==============================++OLD CODE++(Removed db passing to funcs)====================================================

//For converting default time unit of ns to secs
var secs time.Duration = time.Duration(math.Pow10(9))

//Database of key-value pairs
var db = make(map[string]*Data)

//Global mutex for locking the database for the set operations
var globMutex = &sync.RWMutex{}

type Data struct {
	value       string
	version     int64
	numbytes    int64
	setTime     int64
	expiry      int64
	recordMutex *sync.Mutex
	timerRecord *time.Timer
}

func kvStoreProcessing() {
	//reads from commitCh
	//processes the cmds and calculates the result--same as before with few modifications
	//writes the response to conn

	logEntry := <-commitCh
	conn:=connLog[logEntry]
	/*
		--separate data from logEntry--PENDING

	*/
	request := "" //contains extracted command
	str:= strings.Split(request, "\r\n")
	//Server response and value field from command
	sr := ""
	key := ""

	//cmd contains individual fields of command
	cmd := strings.Fields(str[0])
	op := strings.ToLower(cmd[0])
	l := len(cmd)
	//may be empty depending on set or get,doesn't cause a prob as get doesn't use value
	value := str[1]
	if l > 1 {
		key = cmd[1]
	}else{
	    return
	}

	switch op {
	case "set":

		if l == 4 || (l == 5 && cmd[4] == "noreply") {
			numBStr := cmd[3]
			expStr := cmd[2]
			numb, err1 := strconv.ParseInt(numBStr, 0, 64)
			checkErr(err1)
			//value = readNBytes(numb, reader)
			sr = setFields(expStr, numb, value, key, l, op)
		} else {
			sr = "ERR_CMD_ERR\r\n"
		}
	case "get":
		sr = getFields(key, l, op)
	case "getm":
		sr = getFields(key, l, op)
	case "cas":
		if l == 5 || (l == 6 && cmd[5] == "noreply") {
			globMutex.RLock()
			d, exist := db[key]
			if exist != false {
				d.recordMutex.Lock()
				globMutex.RUnlock()
				oldVersion := strconv.FormatInt(d.version, 10)
				d.recordMutex.Unlock()
				newVersion := cmd[3]
				numBStr := cmd[4]
				expStr := cmd[3]
				numb, err1 := strconv.ParseInt(numBStr, 0, 64)
				checkErr(err1)
				if newVersion == oldVersion {
					sr = setFields(expStr, numb, value, key, l, op)
				} else {
					sr = "ERR_VERSION\r\n"
				}
			} else {
				globMutex.RUnlock()
				sr = "ERRNOTFOUND\r\n"
			}
		} else {
			sr = "ERR_CMD_ERR\r\n"
		}
	case "delete":
		sr = deleteRecord(key)
	default:
		sr = "ERRINTERNAL\r\n"
	}


	_, err2 := conn.Write([]byte(sr))
	if err2 != nil {
		return
	}

}

func setFields(expStr string, numb int64, value string, key string, l int, op string) (sr string) {
	//Timer handling
	var oldTimer *time.Timer
	globMutex.RLock()
	d, exist := db[key]
	if exist && d.timerRecord != nil {
		oldTimer = d.timerRecord
	}
	globMutex.RUnlock()
	timer := oldTimer

	//Conversion from string to apt data type
	exp, err := strconv.ParseInt(expStr, 0, 64)
	checkErr(err)

	ver := int64(rand.Intn(10000))

	setTime := time.Now().Unix()

	if exp > 0 {
		if oldTimer != nil {
			oldTimer.Stop()
		}
		expInSec := secs * time.Duration(exp)
		timer = time.AfterFunc(expInSec, func() {
			checkAndExpire(key, exp, setTime)
		})

	}
	globMutex.Lock()
	db[key] = &Data{value, ver, numb, setTime, exp, &sync.Mutex{}, timer}
	globMutex.Unlock()
	if op == "set" {
		if l == 4 {
			sr = "OK " + strconv.FormatInt(ver, 10) + "\r\n"
		} else {
			sr = ""
		}
	} else if op == "cas" {
		if l == 5 {
			sr = "OK " + strconv.FormatInt(ver, 10) + "\r\n"
		} else {
			sr = ""
		}
	}

	return
}

func getFields(key string, l int, op string) (sr string) {
	if l == 2 {
		globMutex.RLock()
		d, exist := db[key]
		if exist != false {
			d.recordMutex.Lock()
			globMutex.RUnlock()
			valueBytes := d.value
			ver := d.version
			numBytes := d.numbytes
			remExp := d.expiry - (time.Now().Unix() - d.setTime)
			d.recordMutex.Unlock()

			verStr := strconv.FormatInt(ver, 10)
			numStr := strconv.FormatInt(numBytes, 10)
			remExpStr := strconv.FormatInt(remExp, 10)

			if op == "getm" {
				sr = "VALUE " + verStr + " " + remExpStr + " " + numStr + "\r\n" + valueBytes + "\r\n"
			} else {
				sr = "VALUE " + numStr + "\r\n" + valueBytes + "\r\n"
			}
		} else {
			globMutex.RUnlock()
			sr = "ERRNOTFOUND\r\n"
		}
	} else {
		sr = "ERR_CMD_ERR\r\n"
	}
	return
}

func deleteRecord(key string) (sr string) {
	globMutex.RLock()
	_, exist := db[key]
	globMutex.RUnlock()
	if exist != false {
		globMutex.Lock()
		delete(db, key)
		globMutex.Unlock()
		sr = "DELETED\r\n"
	} else {
		sr = "ERRNOTFOUND\r\n"
	}
	return
}

func checkAndExpire(key string, oldExp int64, setTime int64) {
	absOldExp := setTime + oldExp
	globMutex.RLock()
	d, exist := db[key]
	if !exist {
		return
	}
	d.recordMutex.Lock()
	globMutex.RUnlock()
	absNewExp := setTime + d.expiry
	d.recordMutex.Unlock()
	if absOldExp == absNewExp {
		globMutex.Lock()
		delete(db, key)
		globMutex.Unlock()
	}
	return

}

func checkErr(err error) {
	if err != nil {
		log.Println("Error encountered:", err)
	}
}

//===============================================ENDS========================================================

//===============================================+++NEW CODE+++============================================

func ServerStart(cluster *ClusterConfig, thisServerId int) {
	commitCh := make(chan LogEntry)
	raftObj,err := NewRaft(cluster, thisServerId, commitCh)
	connHandler(raftObj)
}

func NewRaft(cluster *ClusterConfig, thisServerId int, commitCh chan LogEntry) (raftObj *Raft,err error) {
	err=nil
	var sObj ServerConfig
	//Initialise raftObj
	raftObj = &Raft{cluster, sObj, sObj, 0}
	servArr := cluster.Servers //TO DO-to check pointer access
	//Intialising the ServerConfig struct for itself assuming id is already set in ServerConfig struct
	for i, server := range servArr {
		if i == 0 {
			//read leader details into raftObj
			raftObj.leaderConfig = sObj
			ldrObj := raftObj.leaderConfig 
			ldrObj.Id := thisServerId
			ldrObj.Hostname = "Server" + string(i)
			ldrObj.ClientPort = 800+i
			ldrObj.LogPort = 900+i

		}
		if server.Id == thisServerId {
			//read self details into raftObj
			raftObj.myconfig = sObj
			myObj := raftObj.myconfig
			//Dummy values
			myObj.Hostname = "Server" + string(i)
			myObj.ClientPort = 800+i
			myObj.LogPort = 800+i
		}
	}

	/*
	   publish AppendEntriesRPC service
	*/
	return raftObj,err
}

func connHandler(r *Raft) {
	go listenToServers(r)
	go listenToClients(r)
	//waits to read responseChan
}

func listenToClients(r *Raft) {
	port := r.myconfig.ClientPort
	service := ":" + string(port)
	tcpaddr, err := net.ResolveTCPAddr("tcp", service)
	checkErr(err)
	listener, err := net.ListenTCP("tcp", tcpaddr)
	checkErr(err)
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go handleClient(conn, r)
	}
}

func handleClient(conn net.Conn, r *Raft) {
    var cmd [512]byte
	n,err1 := conn.Read(cmd[0:])
	checkErr(err1)
	var l SharedLog
	l = r
	logEntry, err := l.Append(cmd[0:n])

	//write the logEntry:conn to map
	connLog[logEntry] = conn
	status := 0
	if err == nil {
		commitCh <- logEntry
		status := 1
		go kvStoreProcessing()
	}
	if status == 0 {
		//REDUNTANT: Leader info is already known and present in raftObj, so err is useless for now
		ldrHost, ldrPort := r.leaderConfig.Hostname,r.leaderConfig.ClientPort
		errRedirectStr := "ERR_REDIRECT " + string(ldrHost) + " " + string(ldrPort)
		_, err1 := conn.Write([]byte(errRedirectStr))
	} else {
		//DO NOTHING--as kvstore will write to conn directly after processing is done
		fmt.Println("Kv store will write to conn")
	}
}

func listenToServers(r *Raft) {
	//all servers must listen to their resp ports so port must be passed
	port := r.myconfig.LogPort
	service := ":" + string(port) //listening to its log port
	tcpaddr, err := net.ResolveTCPAddr("tcp", service)
	checkErr(err)
	listener, err := net.ListenTCP("tcp", tcpaddr)
	checkErr(err)
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		//handleServer(conn, db)
	}
}

