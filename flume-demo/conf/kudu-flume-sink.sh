#! /bin/bash

case $1 in
"start"){
         echo " --------启动 kudu-flume-sink 采集 flume-------"
         nohup flume-ng agent --conf-file kudu-flume-sink.conf --name agent -Xmx2048m -Dflume.root.logger=INFO,LOGFILE >> /data/flume-app/logs/kudu-flume-sink.log 2>&1 &
};;
"stop"){
        echo " --------停止 kudu-flume-sink 采集flume-------"
        ps -ef | grep kudu-flume-sink.conf | grep -v grep |awk "{print \$2}" | xargs kill
};;
esac