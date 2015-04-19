package raft

import (
	"encoding/gob"
	"fmt"
	"net"
	"strconv"
	//"time"
	//"log"
)

func (r *Raft) connHandler(f int, e int) {
	//go r.listenToServers()
	go r.listenToServers()
	go r.listenToClients()
	go r.ServerSM(f, e)
	fmt.Println("Launched listeners and SM")
	//time.Sleep(time.Second * 15)

}

//==============+Assign4+===========

//timeout param added Only for testing
func (r *Raft) ServerSM(f int, e int) {
	state := follower //how to define type for this?--const
	for {
		switch state {
		case follower:
			//fmt.Println("in case follower")
			state = r.follower(f)
		case candidate:
			//fmt.Println("in case candidate of ServSM()")
			state = r.candidate(e)
		case leader:
			//fmt.Println("in case leader")
			state = r.leader()
		default:
			return

		}
	}
}

func (r *Raft) listenToServers() {
	fmt.Println("In listen to servers", r.myId())
	port := r.Myconfig.LogPort
	service := r.Myconfig.Hostname + ":" + strconv.Itoa(port)
	tcpaddr, err := net.ResolveTCPAddr("tcp", service)
	if err != nil {
		fmt.Println("Error in listenToServers(),ResolveTCPAddr")
		checkErr("Error in listenToServers(),ResolveTCPAddr", err)
		return
	} else {
		//fmt.Println("No error!")
		listener, err := net.ListenTCP("tcp", tcpaddr)
		if err != nil {
			checkErr("Error in listenToServers(),ListenTCP", err)
			fmt.Println("Error after Listen", err)
			return
		} else {
			for {
				fmt.Println(r.myId(), "listening to servers")
				conn, err := listener.Accept()
				//fmt.Println(r.myId(), "Accepted!,conn:", conn)
				if err != nil {
					//					fmt.Println("Err in listener.Accept")
					checkErr("Error in listenToServers(),Accept", err)
					continue
				} else if conn != nil { //Added if to remove nil pointer reference error
					//					fmt.Println("Connection accepted", r.myId())
					//go r.writeToEvCh(conn)
					//go r.updateReceiverConn(conn)
					go r.writeToEvCh(conn)
				}
			}

		}
	}
}

func (r *Raft) listenToClients() {
	fmt.Println("In listen to client", r.myId())
	port := r.Myconfig.ClientPort
	service := r.Myconfig.Hostname + ":" + strconv.Itoa(port)
	tcpaddr, err := net.ResolveTCPAddr("tcp", service)
	if err != nil {
		fmt.Println("Error in listenToClients(),ResolveTCPAddr", err)
		checkErr("Error in listenToClients(),ResolveTCPAddr", err)
		return
	} else {
		listener, err := net.ListenTCP("tcp", tcpaddr)
		if err != nil {
			fmt.Println("Error in listenToClients(),ListenTCP", err)
			checkErr("Error in listenToClients(),ListenTCP", err)
			return
		} else {
			for {
				fmt.Println("Looping for connection from client", r.myId())
				conn, err := listener.Accept()
				fmt.Println(r.myId(), "Accepted!,conn:", conn)
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

func (r *Raft) writeToEvCh(conn net.Conn) {
	r.registerTypes()
	for {
		msg, err := r.DecodeInterface(conn)
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
		checkErr("In DecodeInterface, err is:", err_dec)
		return nil, err_dec

	}
	//fmt.Printf("After decoding from gob,type is %T \n", obj_dec)
	//	fmt.Printf("In decode interface %v decoded value %T %v \n", r.myId(), obj_dec, obj_dec)
	return obj_dec, nil
}

//to convert client reads and write to conn Read and write rather than gob, so telnet can be used
func (r *Raft) handleClient(conn net.Conn) {
	for {
		var msg [512]byte
		n, err := conn.Read(msg[0:])
		fmt.Println("In handleClient,msg read is:", string(msg[0:]))
		if err == nil && conn != nil {
			fmt.Println("Error in conn read is nil,msg is:", string(msg[0:]))
			cmd := msg
			logEntry, err := r.Append(cmd[0:n])
			//write the logEntry:conn to map
			connMapMutex.Lock()
			connLog[&logEntry] = conn
			connMapMutex.Unlock()

			if err == nil {
				fmt.Println("launched kvstore")
				go r.kvStoreProcessing(&logEntry)

			} else {
				//REDUNTANT: Leader info is already known and present in raftObj, so err is useless for now
				ldrHost, ldrPort := r.LeaderConfig.Hostname, r.LeaderConfig.ClientPort
				errRedirectStr := "ERR_REDIRECT " + ldrHost + " " + strconv.Itoa(ldrPort)
				_, err1 := conn.Write([]byte(errRedirectStr))
				if err1 != nil {
					checkErr("Error in writing redirect string to conn", err1)
				}
				//fmt.Println(r.myId(), "In handleclient, wrote redirection to conn")
			}
		} else {
			fmt.Println("In handleclient, return from method", r.myId())
			return
		}
	}
}
