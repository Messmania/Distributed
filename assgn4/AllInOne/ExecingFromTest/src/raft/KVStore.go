package raft

import (
	//"fmt"
	"log"
	//	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

//For distinguishing of clients by the kvStore
var connLog = make(map[*LogEntry]net.Conn)
var connMapMutex = &sync.RWMutex{}

//==============================++OLD CODE++====================================================

//For converting default time unit of ns to secs
var secs time.Duration = time.Second

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

//func kvStoreProcessing(commitCh chan *LogEntry) {
func kvStoreProcessing(logEntry *LogEntry) {
	//	fmt.Println("In kvprocessing", *logEntry)
	//logEntry := <-commitCh
	connMapMutex.RLock()
	conn := connLog[logEntry]
	connMapMutex.RUnlock()
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
			checkErr("Error in kvStoreProc,strconv.ParseInt(numBStr, 0, 64)", err1)
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
				checkErr("Error in cas", err1)
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

	//	fmt.Println("In kvstoreproc, about to write to conn")
	//_, err2 := conn.Write([]byte(sr))
	EncodeInterface(conn, sr)
	//	fmt.Println("In kvstoreproc, wrote to conn:-", sr, "for cmd:", request)
	//	if err2 != nil {
	//		return
	//	}

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
	checkErr("Error in setFields", err)

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

func checkErr(msg string, err error) {
	if err != nil {
		//log.Println("Error encountered in KVStore.go:", err)
		log.Println(msg, err)

	}
}

//===============================================ENDS========================================================
