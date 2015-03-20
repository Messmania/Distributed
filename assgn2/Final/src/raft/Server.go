package raft

import (
	//"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

//For distinguishing of clients by the kvStore
var connLog = make(map[*LogEntry]net.Conn)

//==============================++OLD CODE++====================================================

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

func kvStoreProcessing(commitCh chan *LogEntry) {
	logEntry := <-commitCh
	conn := connLog[logEntry]
	//separate cmd from logEntry
	request := string((*logEntry).Data())
	str := strings.Split(request, "\r\n")
	//Server response and value field from command
	sr := ""
	key := ""
	op := ""
	l := 0
	value := ""
	//cmd contains individual fields of command
	cmd := strings.Fields(str[0])
	l = len(cmd)
	if l > 0 {
		op = strings.ToLower(cmd[0])
		value = str[1]
		key = cmd[1]
	} else {
		return
	}
	switch op {
	case "set":
		if l == 4 {
			numBStr := cmd[3]
			expStr := cmd[2]
			numb, err1 := strconv.ParseInt(numBStr, 0, 64)
			checkErr(err1)
			sr = setFields(expStr, numb, value, key, l, op)
		} else {
			sr = "ERR_CMD_ERR\r\n"
		}
	case "get":
		sr = getFields(key, l, op)
	case "getm":
		sr = getFields(key, l, op)
	case "cas":
		if l == 5 {
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
		}
	} else if op == "cas" {
		if l == 5 {
			sr = "OK " + strconv.FormatInt(ver, 10) + "\r\n"
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
		globMutex.RUnlock()
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
		log.Println("Error encountered in server.go:", err)

	}
}

//===============================================ENDS========================================================

//===============================================+++NEW CODE+++============================================

func ServerStart(cluster *ClusterConfig, thisServerId int) {
	commitCh := make(chan *LogEntry, 4)
	raftObj, err := NewRaft(cluster, thisServerId, commitCh)
	if err != nil {
		checkErr(err)
		return
	} else {
		connHandler(raftObj)
	}
}

func NewRaft(cluster *ClusterConfig, thisServerId int, commitCh chan *LogEntry) (raftObj *Raft, err error) {
	err = nil
	var myObj ServerConfig
	var leaderObj ServerConfig

	//Intialising the ServerConfig struct for itself assuming id is already set in ServerConfig struct
	servArr := (*cluster).Servers
	for i, server := range servArr {
		if i == 0 {
			//read leader details into raftObj
			leaderObj.Id = servArr[i].Id
			leaderObj.Hostname = servArr[i].Hostname
			leaderObj.ClientPort = servArr[i].ClientPort
			leaderObj.LogPort = servArr[i].LogPort

		}

		if server.Id == thisServerId {
			//read leader details into raftObj
			myObj.Id = thisServerId
			myObj.Hostname = servArr[i].Hostname
			myObj.ClientPort = servArr[i].ClientPort
			myObj.LogPort = servArr[i].LogPort
		}
	}

	//Initialise raftObj
	raftObj = &Raft{*cluster, myObj, leaderObj, 0, commitCh}

	//  publish AppendEntriesRPC service for itself only
	RegisterTCP_RPCService(myObj.Hostname, myObj.LogPort)
	return raftObj, err
}

func connHandler(r *Raft) {
	//go listenToServers(r)   no need of this as it is already done in RegisterTCP_RPCService
	listenToClients(r)
}

func listenToClients(r *Raft) {
	port := r.Myconfig.ClientPort
	service := r.Myconfig.Hostname + ":" + strconv.Itoa(port)
	tcpaddr, err := net.ResolveTCPAddr("tcp", service)
	if err != nil {
		checkErr(err)
		return
	} else {
		listener, err := net.ListenTCP("tcp", tcpaddr)
		if err != nil {
			checkErr(err)
			return
		} else {
			for {
				conn, err := listener.Accept()
				if err != nil {
					continue
				} else if conn != nil { //Added if to remove nil pointer reference error
					go handleClient(conn, r)
				}
			}

		}
	}
}

func handleClient(conn net.Conn, r *Raft) {
	for {
		var cmd [512]byte
		n, err1 := conn.Read(cmd[0:])
		if err1 != nil {
			break
		} else {
			var l SharedLog
			l = r
			logEntry, err := l.Append(cmd[0:n])
			//write the logEntry:conn to map
			connLog[&logEntry] = conn

			if err == nil {
				r.commitCh <- (&logEntry)
				go kvStoreProcessing(r.commitCh)

			} else {
				//REDUNTANT: Leader info is already known and present in raftObj, so err is useless for now
				ldrHost, ldrPort := r.LeaderConfig.Hostname, r.LeaderConfig.ClientPort
				errRedirectStr := "ERR_REDIRECT " + ldrHost + " " + strconv.Itoa(ldrPort)
				_, err1 := conn.Write([]byte(errRedirectStr))
				checkErr(err1)
			}
		}
	}
}
