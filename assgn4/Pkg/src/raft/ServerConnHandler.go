package raft

//package main

import (
	"encoding/gob"
	//  "fmt"
	"net"
	"strconv"
	//"time"
)

func (r *Raft) connHandler(timeout int) {
	go r.listenToServers()
	go r.listenToClients()
	go r.ServerSM(timeout)
	//  fmt.Println("Launched listeners and SM")
}

//==============+Assign4+===========

func (r *Raft) listenToServers() {
	//fmt.Println("In listen to servers", r.myId())
	port := r.Myconfig.LogPort
	service := r.Myconfig.Hostname + ":" + strconv.Itoa(port)
	tcpaddr, err := net.ResolveTCPAddr("tcp", service)
	if err != nil {
		checkErr(err)
		return
	} else {
		//fmt.Println("No error!")
		listener, err := net.ListenTCP("tcp", tcpaddr)
		if err != nil {
			checkErr(err)
			//fmt.Println("Error after Listen")
			return
		} else {
			for {
				//fmt.Println(r.myId(), "listening to servers")
				conn, err := listener.Accept()
				//fmt.Println(r.myId(), "Accepted!,conn:", conn)
				if err != nil {
					//                  fmt.Println("Err in listener.Accept")
					checkErr(err)
					continue
				} else if conn != nil { //Added if to remove nil pointer reference error
					go r.writeToEvCh(conn)
				}
			}

		}
	}
}

func (r *Raft) listenToClients() {
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
				//fmt.Println("Looping for connection from client")
				conn, err := listener.Accept()
				//fmt.Println(r.myId(), "Accepted!,conn:", conn)
				if err != nil {
					continue
				} else if conn != nil { //Added if to remove nil pointer reference error
					//                  fmt.Println("Connection made at server side", r.myId())
					go r.handleClient(conn)
				}
			}

		}
	}
}

//Decode is not blocking, and for loops runs infinitely, shouldnt it be conn.Read and Write so that it is blocking and
//reads only when something is written?
func (r *Raft) writeToEvCh(conn net.Conn) {
	//fmt.Println("In write to evCh", r.myId())
	registerTypes()
	msg := DecodeInterface(conn)
	//conn.Close() //--moved here from DecodeInterface so that handleClient can use DecodeInterface too
	r.EventCh <- msg
}

func registerTypes() {
	gob.Register(AppendEntriesReq{})
	gob.Register(AppendEntriesResponse{})
	gob.Register(RequestVote{})
	gob.Register(RequestVoteResponse{})
	gob.Register(ClientAppendReq{})
	gob.Register(ClientAppendResponse{})
}

//For decoding the values
func DecodeInterface(conn net.Conn) interface{} {
	dec_net := gob.NewDecoder(conn)
	var obj_dec interface{}
	err_dec := dec_net.Decode(&obj_dec)
	if err_dec != nil {
		//fmt.Println("Err in decoding", err_dec)
		return nil
	}
	//fmt.Printf("After decoding from gob,type is %T \n", obj_dec)
	//fmt.Printf("decoded value %T %v \n", obj_dec, obj_dec)
	return obj_dec
}

func (r *Raft) handleClient(conn net.Conn) {
	//for {
	//if conn != nil { //keep looping till client closes the conn, usefull only if for loop is uncommented
	//fmt.Println("In handleClient", r.myId())
	msg := DecodeInterface(conn)
	//msg := Decode(conn) // for testing only
	//fmt.Println("In handleClient,msg decoded is:", msg)
	if msg != nil {
		cmd := []byte(msg.(string))
		//fmt.Println("Decoded cmd is:", msg.(string))
		logEntry, err := r.Append(cmd)
		//fmt.Println("In handleClient after calling append")
		//write the logEntry:conn to map
		connMapMutex.Lock()
		connLog[&logEntry] = conn
		connMapMutex.Unlock()

		if err == nil {
			//r.CommitCh <- (&logEntry)
			//fmt.Println("KVStore launched")
			//go kvStoreProcessing(r.CommitCh)
			go kvStoreProcessing(&logEntry)

		} else {
			//REDUNTANT: Leader info is already known and present in raftObj, so err is useless for now
			ldrHost, ldrPort := r.LeaderConfig.Hostname, r.LeaderConfig.ClientPort
			errRedirectStr := "ERR_REDIRECT " + ldrHost + " " + strconv.Itoa(ldrPort)
			EncodeInterface(conn, errRedirectStr)
			//_, err1 := conn.Write([]byte(errRedirectStr))
			//checkErr(err1)
		}
	}
	//}
	//}
}
