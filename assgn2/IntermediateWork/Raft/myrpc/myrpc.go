package myrpc

//import "errors"
import "fmt"

type Args struct {
    Cmd string
}

type RPCservice int

func (t *RPCservice) AppendEntriesRPC(args *Args, reply *bool) error {
	fmt.Println("in AppendEntries*****")
    *reply = true
    	fmt.Println("reply: ", *reply)
    return nil
}


