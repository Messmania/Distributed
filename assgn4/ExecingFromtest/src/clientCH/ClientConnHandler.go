package clientCH

import (
	//"encoding/gob"
	//"fmt"
	"log"
	"net"
	"strconv"
	"strings"
)

func Client(ch chan string, strEcho string, hostname string, port int) {
	service := hostname + ":" + strconv.Itoa(port)
	tcpAddr, err := net.ResolveTCPAddr("tcp", service)
	if err != nil {
		checkErr("Error in Client(), ResolveTCPAddr", err)
		return
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		checkErr("Error in Client(),DailTCP", err)
		return
	} else {
		cmd := SeparateCmds(strEcho)
		for i := 0; i < len(cmd); i++ {
			_, err := conn.Write([]byte(cmd[i]))
			if err != nil {
				checkErr("In Client(),Error in conn.Write", err)
				return
			}
			var rep [512]byte
			n, err1 := conn.Read(rep[0:])
			if err1 != nil {
				checkErr("In Client(),Error in Reading from conn", err1)
			}
			reply := string(rep[0:n])
			ch <- reply
		}
	}
	err1 := conn.Close()
	if err1 != nil {
		checkErr("Error in Client, closing conn", err1)
	}
}

//For separating multiple cmds in a single string (MRMC case)
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
		} else {
			newC = line[j]
			cmd = append(cmd, newC)
			ptr++
		}
	}
	return
}

func checkErr(msg string, err error) {
	if err != nil {
		log.Println(msg, err)
	}
}
