package main

import (
    "os"
    "raft"
    "strconv"
    //"fmt"
    "strings"
)

func main() {
    //fmt.Println("inside")
    args := os.Args
    vstr := args[1]

    var v raft.ClusterConfig
    path_servers := strings.Split(vstr, ".")
    v.Path = path_servers[0]
    servers := strings.Split(path_servers[1], ";")
    //fmt.Println(servers)
    i := 0
    for sid := range servers {

        server_fields := strings.Split(servers[sid], ",")
        var sc raft.ServerConfig
        for fid := range server_fields {
            if fid == 0 {
                //fmt.Print(server_fields[fid] ," ")
                sc.Id, _ = strconv.Atoi(server_fields[fid])
            }
            if fid == 1 {
                //fmt.Print(server_fields[fid] , " ")
                sc.Hostname = server_fields[fid]
            }
            if fid == 2 {
                //fmt.Print(server_fields[fid] ," ")
                sc.ClientPort, _ = strconv.Atoi(server_fields[fid])
            }
            if fid == 3 {
                //fmt.Print(server_fields[fid] ," ")
                sc.LogPort, _ = strconv.Atoi(server_fields[fid])
            }
        }
        //fmt.Println("")
        v.Servers = append(v.Servers, sc)
        i = i + 1
    }
    //fmt.Println("inside")
    thisServerId, _ := strconv.Atoi(args[2])
    raft.ServerStart(&v, thisServerId)
    //fmt.Println("exiting")
}
