Steps to install and run:
 
1. Set gopath to point to parent of src folder say X
    export GOPATH=<path>/X
2. Set PATH to include the gopath's bin
    export PATH=$PATH:$GOPATH/bin
3. go install serverStarter
4. go install Read_Exec
5. Read_Exec <json file's absolute path>
6. go test asg2_Test
