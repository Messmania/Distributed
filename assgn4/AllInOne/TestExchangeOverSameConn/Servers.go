package main

import (
	"encoding/gob"
	"fmt"
	"net"
	"strconv"
	"time"
)

const layout = "3:04:5 pm (MST)"

func Sender(conn net.Conn) {
	fmt.Println("in sender, conn is:", conn)
	enc_net := gob.NewEncoder(conn)
	var obj_enc interface{}
	//time.Sleep(time.Second * 10) //to check if Decode is blocking--Yes it is!
	//conn.Close() //for testing what happens to Decode--err is EOF
	//var obj_enc Response1
	obj_enc = Response1{1, "response"}
	fmt.Println("Object encoded is:", obj_enc)
	err_enc := enc_net.Encode(&obj_enc)
	if err_enc != nil {
		fmt.Println("Err in encoding", err_enc)
	}
	obj_enc = Reply1{2, "reply"}
	fmt.Println("Object encoded is:", obj_enc)
	enc_net.Encode(&obj_enc)
}
func Receiver(conn net.Conn) {
	fmt.Printf("in receiver,conn is of type %T \n", conn)
	gob.Register(Reply1{})
	gob.Register(Response1{})
	dec_net := gob.NewDecoder(conn)
	var obj_dec interface{}
	//var obj_dec Reply1
	fmt.Println("before decoding,obj_dec", obj_dec)
	fmt.Println("Before decoding,time is:", time.Now().Format(layout))
	err_dec := dec_net.Decode(&obj_dec)
	fmt.Println("After decoding,time is:", time.Now().Format(layout))
	if err_dec != nil {
		fmt.Println("Err in decoding", err_dec)
		return
	}
	//fmt.Printf("After decoding from gob,type is %T \n", obj_dec)
	fmt.Printf("decoded value %T %v \n", obj_dec, obj_dec)
	dec_net.Decode(&obj_dec)
	fmt.Printf("decoded value %T %v \n", obj_dec, obj_dec)
	conn.Close()
	return
}

type Response1 struct {
	Page   int
	Fruits string
}

type Reply1 struct {
	Page   int
	Fruits string
}

func makeConnection(port int) {
	service := ":" + strconv.Itoa(port)
	tcpAddr, _ := net.ResolveTCPAddr("tcp", service)
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		fmt.Println("Err in dialing", err)
		return
	}
	fmt.Printf("type of conn is %T \n", conn)
	if conn == nil {
		fmt.Println("Conn is nil in makeConn")
	} else {

		Sender(conn)
	}

	return
}

func listenOnPort(port int) {
	service := ":" + strconv.Itoa(port)
	tcpaddr, err := net.ResolveTCPAddr("tcp", service)
	if err != nil {
		return
	} else {
		//fmt.Println("No error!")
		listener, err := net.ListenTCP("tcp", tcpaddr)
		if err != nil {
			return
		} else {
			for {
				conn, err := listener.Accept()
				if err != nil {
					fmt.Println("Error in accepting", err)
					return
				} else if conn != nil { //Added if to remove nil pointer reference error
					go Receiver(conn)
				}
			}

		}
	}
}

func main() {
	fmt.Println("=====GOB with conn========")
	go listenOnPort(9000)
	time.Sleep(time.Microsecond * 1)
	go makeConnection(9000)
	time.Sleep(time.Second * 20)
}
