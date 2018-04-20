#!/bin/sh

#helpInfo=' This is a generic command line parser demo.
# USAGE EXAMPLE:  ./flume-start.sh  "flume agentName of you want to stop" '


#if [ ! -n "$1" ] || [ -n "$1" -a "$1" == "-h" ];
#        then echo $helpInfo
#        exit 1;
#else
#        agentName=$1
#fi

#不指定参数，查找该包默认固定的源配置文件
#执行脚本的路径
curdir=$(cd $(dirname ${BASH_SOURCE[0]});pwd)
echo ${curdir}
cd ${curdir}
configFile=`find ../conf/ -name "flume_*\.conf"`
dos2unix $configFile
grepStr="agentName="

#获得agentName
agentName=$(grep $grepStr $configFile | cut -d = -f 2)
echo ${agentName}

jps -m | grep "Application -n ${agentName}" | awk '{ print $1 }' | xargs -r kill -9  




