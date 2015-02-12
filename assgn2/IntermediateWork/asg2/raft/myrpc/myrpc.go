package myrpc


type Args struct {
    Cmd string
}

type RPCservice int

func (t *RPCservice) AppendEntriesRPC(args *Args, reply *bool) error {
    *reply = true
    return nil
}


