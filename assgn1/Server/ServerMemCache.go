package Server

import (
	"log"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

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

func Server() {
	service := ":9000"
	tcpaddr, err := net.ResolveTCPAddr("tcp", service)
	checkErr(err)
	listener, err := net.ListenTCP("tcp", tcpaddr)
	checkErr(err)
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleClient(conn, db)
	}
}

func handleClient(conn net.Conn, db map[string]*Data) {
	for true {
		var buf [512]byte
		n, err := conn.Read(buf[0:])
		if err != nil {
			return
		}
		//Server response
		sr := ""

		//Variable line contains one or two lines of cmds, cmd contains individual fields of command
		str := string(buf[0:n])
		line := strings.Split(str, "\r\n")
		cmd := strings.Fields(line[0])
		op := strings.ToLower(cmd[0])
		value := line[1]
		l := len(cmd)
		key := cmd[1]

		switch op {
		case "set":
			if l == 4 || (l == 5 && cmd[4] == "noreply") {
				numBStr := cmd[3]
				expStr := cmd[2]
				sr = setFields(db, expStr, numBStr, value, key, l, op)
			} else {
				sr = "ERR_CMD_ERR\r\n"
			}
		case "get":
			sr = getFields(db, key, l, op)
		case "getm":
			sr = getFields(db, key, l, op)
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
					if newVersion == oldVersion {
						sr = setFields(db, expStr, numBStr, value, key, l, op)
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
			sr = deleteRecord(db, key)
		default:
			sr = "ERRINTERNAL\r\n"
		}

		_, err2 := conn.Write([]byte(sr))
		if err2 != nil {
			return
		}
	}
}

func setFields(db map[string]*Data, expStr string, numBStr string, value string, key string, l int, op string) (sr string) {
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
	numb, err1 := strconv.ParseInt(numBStr, 0, 64)
	checkErr(err1)

	//For incremental version numbers--TO BE ADDED
	ver := int64(rand.Intn(10000))
	if numb != int64(len(value)) {
		numb = int64(len(value))
	}
	setTime := time.Now().Unix()

	if exp > 0 {
		if oldTimer != nil {
			oldTimer.Stop()
		}
		expInSec := secs * time.Duration(exp)
		timer = time.AfterFunc(expInSec, func() {
			checkAndExpire(db, key, exp, setTime)
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

func getFields(db map[string]*Data, key string, l int, op string) (sr string) {
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

func deleteRecord(db map[string]*Data, key string) (sr string) {
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

func checkAndExpire(db map[string]*Data, key string, oldExp int64, setTime int64) {
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

func Client(ch chan string, strEcho string, c string) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", ":9000")
	checkErr(err)
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	checkErr(err)
	cmd := SeparateCmds(strEcho)
	for i := 0; i < len(cmd); i++ {
		conn.Write([]byte(cmd[i]))
		var rep [512]byte
		n, err1 := conn.Read(rep[0:])
		checkErr(err1)
		reply := string(rep[0:n])
		ch <- reply
	}
	conn.Close()
}

func SeparateCmds(str string) (cmd []string) {
	line := strings.Split(str, "\r\n")
	count := len(line) - 1
	args := make([][]string, count)
	for i := 0; i < count; i++ {
		args[i] = strings.Fields(line[i])
	}
	ptr := 0
	cmd = make([]string, 0)
	var newC string
	for j := 0; j < count; j++ {
		op := strings.ToLower(args[ptr][0])
		if (op == "set") || (op == "cas") {
			newC := line[ptr] + "\r\n" + line[ptr+1] + "\r\n"
			cmd = append(cmd, newC)
			ptr = ptr + 2
			j++
		} else if (op == "get") || (op == "getm") || (op == "delete") {
			newC = line[ptr] + "\r\n"
			cmd = append(cmd, newC)
			ptr++
		} else if op == "end" {
			cmd = append(cmd, op)
			break
		} else {
			newC = line[j]
			cmd = append(cmd, newC)
			ptr++
		}
	}
	return
}

func checkErr(err error) {
	if err != nil {
		log.Println("Error encountered:", err)
	}
}
