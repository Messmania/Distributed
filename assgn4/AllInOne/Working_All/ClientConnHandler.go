//package clientCH
package raft //for now, later keep it in above pkg

import (
	//"fmt"
	"log"
	"net"
	"strconv"
	"strings"
)

func Client(ch chan string, strEcho string, hostname string, port int) {
	service := hostname + ":" + strconv.Itoa(port)
	tcpAddr, err := net.ResolveTCPAddr("tcp", service)
	checkErr("Error in Client(), ResolveTCPAddr", err)
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	//for testing
	//conn.Close()
	if err != nil {
		checkErr("Error in Client(),DailTCP", err)
		return
	} else {
		//fmt.Println("Conn dialed at client side,conn:", conn)
		cmd := SeparateCmds(strEcho)
		//		fmt.Println("cmd separated is:", cmd)
		for i := 0; i < len(cmd); i++ {
			msg := cmd[i]
			EncodeInterface(conn, msg)
			//fmt.Println("Encoding done, in Client connH", msg)
			rep, err := DecodeInterface(conn)
			if err != nil {
				log.Println("Error in Client(),", err)
				return
			}
			//			fmt.Println("Decoding done, in Client connH", rep)
			reply := rep.(string)

			//fmt.Println("In client, reply is", reply)
			ch <- reply
		}
	}
	err1 := conn.Close()
	checkErr("Error in Client, closing conn", err1)
	//	fmt.Println("In client, conn closed", conn, err1)
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

/*
func checkErr(err error) {
	if err != nil {
		log.Println("Error encountered in connHandler:", err)

	}
} */
