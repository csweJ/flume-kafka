#!/bin/sh

#helpInfo=' This is a generic command line parser demo. 
# USAGE EXAMPLE:  ./flume-start.sh  "configfile of you want flume source" '
 
  
#if [ ! -n "$1" ] || [ -n "$1" -a "$1" == "-h" ];
#	then echo $helpInfo
#	exit 1;
#else
#        configFile=$1
#        dos2unix $configFile		 
#fi

#不指定参数，查找该包默认固定的源配置文件
#执行脚本的路径
curdir=$(cd $(dirname ${BASH_SOURCE[0]});pwd)
echo ${curdir}
cd ${curdir}
configFile=`find ../conf/ -name "flume_*\.conf"`
dos2unix $configFile

#获取配置文件目录2
configDir=$(dirname $configFile)
echo $configDir

grepStr="agentName="

#获得agentName 
agentName=$(grep $grepStr $configFile | cut -d = -f 2)

#获取行号
line=`sed -n "/${grepStr}/=" $configFile`

sed -i "${line}c #agentName=${agentName}" $configFile
echo "agentName=$agentName"


$curdir/flume-ng agent -n $agentName -c $configDir -f $configFile -Dflume.root.logger=INFO,console
