package clientCH

//package raft //for now, later keep it in above pkg

import (
	//"fmt"
	"encoding/gob"
	"log"
	"net"
	"raft"
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

func checkErr(msg string, err error) {
	if err != nil {
		log.Println(msg, err)

	}
}

func EncodeInterface(conn net.Conn, msg interface{}) {
	//	fmt.Println("in sender, conn is:", conn)
	enc_net := gob.NewEncoder(conn)
	//	fmt.Println("Object encoded is:", obj_enc)
	err_enc := enc_net.Encode(&msg)
	if err_enc != nil {
		//		fmt.Println("Err in encoding", err_enc)
	}
}

func registerTypes() {
	gob.Register(raft.AppendEntriesReq{})
	gob.Register(raft.AppendEntriesResponse{})
	gob.Register(raft.RequestVote{})
	gob.Register(raft.RequestVoteResponse{})
	gob.Register(raft.ClientAppendReq{})
	gob.Register(raft.ClientAppendResponse{})
}

//For decoding the values
func DecodeInterface(conn net.Conn) (interface{}, error) {
	registerTypes()
	dec_net := gob.NewDecoder(conn)
	var obj_dec interface{}
	err_dec := dec_net.Decode(&obj_dec)
	if err_dec != nil {
		//fmt.Println("Err in decoding", err_dec)
		log.Println("In DecodeInterface, err is:", err_dec)
		return nil, err_dec

	}
	//fmt.Printf("After decoding from gob,type is %T \n", obj_dec)
	//fmt.Printf("decoded value %T %v \n", obj_dec, obj_dec)
	return obj_dec, nil
}
