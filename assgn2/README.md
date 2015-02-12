Server-Client architecture based on RAFT(SharedLog) structure with only KVStore replication and RPC calls for now.

Steps to install and run:
 
1. Set gopath to point to parent of src folder say X
    export GOPATH=<path>/X
    
2. Set PATH to include the gopath's bin

    export PATH=$PATH:$GOPATH/bin
    
3. Install the binaries
    
    go install serverStarter
    
    go install Read_Exec  
       
5. Starts the servers
    
    Read_Exec <json file's absolute path>

6. run Test file
    
    go test asg2_Test
