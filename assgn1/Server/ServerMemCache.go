package Server

import (
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var secs time.Duration = time.Duration(math.Pow10(9))
var m = make(map[string]Data)
var globMutex = &sync.Mutex{}

type Data struct {
	value    string
	version  int64
	numbytes int64
	setTime  int64
	expiry   int64
	dbMutex  *sync.Mutex
	//timer			tm
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
		go handleClient(conn, m)
	}
}

func handleClient(conn net.Conn, m map[string]Data) {
	for true {
		var buf [512]byte
		n, err := conn.Read(buf[0:])
		if err != nil {
			return
		}
		sr := ""
		/*Convert command read from conn to array of strings then read them separately	*/
		str := string(buf[0:n])
		line := strings.Split(str, "\r\n")
		cmd := strings.Fields(line[0])
		op := strings.ToLower(cmd[0])
		value := line[1]
		l := len(cmd)
		key := cmd[1]

		switch op {
		case "set":
			if l == 4 {
				exp, err := strconv.ParseInt(cmd[2], 0, 64)
				checkErr(err)
				numb, err1 := strconv.ParseInt(cmd[3], 0, 64)
				checkErr(err1)
				ver := int64(rand.Intn(10000))
				if numb != int64(len(value)) {
					numb = int64(len(value))
				}
				globMutex.Lock()
				m[key] = Data{value, ver, numb, time.Now().Unix(), exp, &sync.Mutex{}}
				d := m[key]
				if exp > 0 {
					expInSec := secs * time.Duration(exp)
					time.AfterFunc(expInSec, func() {
						checkAndExpire(m, key, exp, d.setTime)
					})
				}
				globMutex.Unlock()
				sr = "OK " + strconv.FormatInt(d.version, 10) + "\r\n"
			} else if l == 5 && cmd[4] == "noreply" {
				exp, err := strconv.ParseInt(cmd[2], 0, 64)
				checkErr(err)
				numb, err1 := strconv.ParseInt(cmd[3], 0, 64)
				checkErr(err1)
				ver := int64(rand.Intn(10000))
				if numb != int64(len(value)) {
					numb = int64(len(value))
				}
				globMutex.Lock()
				m[key] = Data{value, ver, numb, time.Now().Unix(), exp, &sync.Mutex{}}
				d := m[key]
				if exp > 0 {
					expInSec := secs * time.Duration(exp)
					time.AfterFunc(expInSec, func() {
						checkAndExpire(m, key, exp, d.setTime)
					})
				}
				globMutex.Unlock()
				sr = ""
			} else {
				sr = "ERR_CMD_ERR\r\n" //wrong command line format
			}
		case "get":
			if l == 2 {
				//do get processing
				globMutex.Lock()
				d, exist := m[key]
				if exist != false {
					globMutex.Unlock()
					d.dbMutex.Lock()
					numStr := strconv.FormatInt(d.numbytes, 10)
					sr = "VALUE " + numStr + "\r\n" + d.value + "\r\n"
					d.dbMutex.Unlock()
				} else {
					globMutex.Unlock()
					sr = "ERRNOTFOUND\r\n"
				}
			} else {
				sr = "ERR_CMD_ERR\r\n"
			}
		case "getm":
			if l == 2 {
				//do getm processing
				globMutex.Lock()
				d, exist := m[key]
				if exist != false {
					globMutex.Unlock()
					d.dbMutex.Lock()
					remExp := d.expiry - (time.Now().Unix() - d.setTime)
					verStr := strconv.FormatInt(d.version, 10)
					sr = "VALUE " + verStr + " " + strconv.FormatInt(remExp, 10) + " " + strconv.FormatInt(d.numbytes, 10) + "\r\n" + d.value + "\r\n"
					d.dbMutex.Unlock()
				} else {
					globMutex.Unlock()
					sr = "ERRNOTFOUND\r\n"
				}
			} else {
				sr = "ERR_CMD_ERR\r\n"
			}
		case "cas":
			if l == 5 {
				globMutex.Lock()
				d, exist := m[key]
				if exist != false {
					globMutex.Unlock()
					d.dbMutex.Lock()
					oldVersion := strconv.FormatInt(d.version, 10)
					newVersion := cmd[3]
					numbytes := cmd[4]
					if newVersion == oldVersion {
						numBInt, err1 := strconv.ParseInt(numbytes, 10, 64)
						checkErr(err1)
						exp, err2 := strconv.ParseInt(cmd[2], 0, 64)
						checkErr(err2)
						ver := int64(rand.Intn(10000))
						if numBInt != int64(len(value)) {
							numBInt = int64(len(value))
						}
						m[key] = Data{value, ver, numBInt, time.Now().Unix(), exp, &sync.Mutex{}}
						a := m[key]
						if exp > 0 {
							expInSec := secs * time.Duration(exp)
							time.AfterFunc(expInSec, func() {
								checkAndExpire(m, key, exp, a.setTime)
							})
						}
						sr = "OK " + strconv.FormatInt(ver, 10) + "\r\n"
					} else {
						sr = "ERR_VERSION\r\n"
					}
					d.dbMutex.Unlock()
				} else {
					globMutex.Unlock()
					sr = "ERRNOTFOUND\r\n"
				}
			} else if l == 6 && cmd[5] == "noreply" {
				sr = ""
			} else {
				sr = "ERR_CMD_ERR\r\n"
			}
		case "delete":
			globMutex.Lock()
			d, exist := m[key]
			if exist != false {
				globMutex.Unlock()
				d.dbMutex.Lock()
				delete(m, key)
				d.dbMutex.Unlock()
				sr = "DELETED\r\n"
			} else {
				globMutex.Unlock()
				sr = "ERRNOTFOUND\r\n"
			}
		default:
			sr = "ERRINTERNAL\r\n"
		}
		_, err2 := conn.Write([]byte(sr))
		if err2 != nil {
			return
		}
	}
}

func checkErr(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func checkAndExpire(m map[string]Data, key string, oldExp int64, setTime int64) {
	absOldExp := setTime + oldExp
	globMutex.Lock()
	d, exist := m[key]
	if exist == false {
		return
	}
	globMutex.Unlock()
	d.dbMutex.Lock()
	absNewExp := setTime + d.expiry
	if absOldExp == absNewExp {
		delete(m, key)
	}
	d.dbMutex.Unlock()
	return

}

func Client(ch chan string, strEcho string, c string) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", ":9000")
	checkErr(err)
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	checkErr(err)
	cmd := SeparateCmds(strEcho)
	for i := 0; i < len(cmd); i++ {
		conn.Write([]byte(cmd[i])) //Writing the message to connection stream which server can read
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
