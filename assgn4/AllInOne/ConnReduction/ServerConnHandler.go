package raft

import (
	"encoding/gob"
	//"fmt"
	"net"
	"strconv"
	//"time"
	//"log"
	"errors"
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
		checkErr("Error in listenToServers(),ResolveTCPAddr", err)
		return
	} else {
		//fmt.Println("No error!")
		listener, err := net.ListenTCP("tcp", tcpaddr)
		if err != nil {
			checkErr("Error in listenToServers(),ListenTCP", err)
			//fmt.Println("Error after Listen")
			return
		} else {
			for {
				//fmt.Println(r.myId(), "listening to servers")
				conn, err := listener.Accept()
				//fmt.Println(r.myId(), "Accepted!,conn:", conn)
				if err != nil {
					//					fmt.Println("Err in listener.Accept")
					checkErr("Error in listenToServers(),Accept", err)
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
		checkErr("Error in listenToClients(),ResolveTCPAddr", err)
		return
	} else {
		listener, err := net.ListenTCP("tcp", tcpaddr)
		if err != nil {
			checkErr("Error in listenToClients(),ListenTCP", err)
			return
		} else {
			for {
				//fmt.Println("Looping for connection from client")
				conn, err := listener.Accept()
				//fmt.Println(r.myId(), "Accepted!,conn:", conn)
				if err != nil {
					continue
				} else if conn != nil { //Added if to remove nil pointer reference error
					//fmt.Println("Connection made at server side", r.myId())
					go r.handleClient(conn)
				}
			}

		}
	}
}

func (r *Raft) getIdAndSetMap(conn net.Conn) {
	msg, err := r.DecodeInterface(conn)
	if err != nil {
		checkErr("Error in getIdAndSetMap", err)
	}
	senderId := msg.(int)
	//fmt.Println(r.myId(), "In get set id ,msg decoded is:", msg, "from:", senderId)
	r.setSenderConn(senderId, conn)
}

func (r *Raft) writeToEvCh(conn net.Conn) {
	r.registerTypes()
	//r.getIdAndSetMap(conn)
	//fmt.Println("Connection accepted", r.myId())
	for {
		//		fmt.Println(r.myId(), "In for loop of writeToEvCh")
		msg, err := r.DecodeInterface(conn)
		//		fmt.Println(r.myId(), "In for loop of writeToEvCh,decoded value is", msg)
		if err != nil {
			checkErr("Error in writeToEvCh(),DecodeInterface", err)
			return
		}
		r.EventCh <- msg
	}
}

func (r *Raft) registerTypes() {
	gob.Register(AppendEntriesReq{})
	gob.Register(AppendEntriesResponse{})
	gob.Register(RequestVote{})
	gob.Register(RequestVoteResponse{})
	gob.Register(ClientAppendReq{})
	gob.Register(ClientAppendResponse{})
}

//For decoding the values
func (r *Raft) DecodeInterface(conn net.Conn) (interface{}, error) {
	dec_net := gob.NewDecoder(conn)
	var obj_dec interface{}
	//	fmt.Println(r.myId(), "In decodeInterface")
	err_dec := dec_net.Decode(&obj_dec)
	if err_dec != nil {
		errVal := errors.New("EOF")
		if err_dec.Error() == errVal.Error() {
			checkErr("Connection closed by client", err_dec)
		} else {
			checkErr("In DecodeInterface, err is:", err_dec)
		}
		return nil, err_dec

	}
	//fmt.Printf("After decoding from gob,type is %T \n", obj_dec)
	//	fmt.Printf("%v,In decode interface:decoded value %T %v \n", r.myId(), obj_dec, obj_dec)
	return obj_dec, nil
}

func (r *Raft) handleClient(conn net.Conn) {
	for {
		//if conn != nil { //keep looping till client closes the conn, usefull only if for loop is uncommented
		//fmt.Println("In handleClient", r.myId())
		msg, err := r.DecodeInterface(conn)
		//fmt.Println("In handleClient,msg decoded is:", msg)
		if err == nil {
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
				go r.kvStoreProcessing(&logEntry)

			} else {
				//REDUNTANT: Leader info is already known and present in raftObj, so err is useless for now
				ldrHost, ldrPort := r.LeaderConfig.Hostname, r.LeaderConfig.ClientPort
				errRedirectStr := "ERR_REDIRECT " + ldrHost + " " + strconv.Itoa(ldrPort)
				r.EncodeInterface(conn, errRedirectStr)
				//_, err1 := conn.Write([]byte(errRedirectStr))
				//checkErr(err1)
			}
		} else { //it means, conn is closed by client
			//			fmt.Println("Exiting handleClient since conn is closed")
			break
		}
	}
}
