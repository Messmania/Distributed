#!/bin/bash
i=9000
while [ $i -lt 9005 ];do
    p=`netstat -anp | grep 127.0.0.1:$i | grep LISTEN | cut -d" " -f47 | cut -d"/" -f1`
    echo -n $p " "
    #`sudo kill -9`+" "+ $p
    i=$((i+1))
done


i=8000
while [ $i -lt 8005 ];do
        p=`sudo netstat -anp | grep 127.0.0.1:$i | grep LISTEN | cut -d" " -f47 | cut -d"/" -f1`
	#p=`sudo netstat -anp | grep 127.0.0.1:$i | grep CLOSE_WAIT | cut -d" " -f47 | cut -d"/" -f1`
        echo -n $p " "
    i=$((i+1))
done
echo