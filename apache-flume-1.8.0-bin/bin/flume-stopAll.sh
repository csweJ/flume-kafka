#!/bin/sh
ps aux | grep flume | awk '{print $2}' | xargs -r kill -9

