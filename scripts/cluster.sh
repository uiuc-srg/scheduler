#!/bin/bash
nohup java -Xmx90G -cp code/scheduler-1.0-SNAPSHOT.jar:code/scheduler-libs.jar edu.illinois.cs.srg.cluster.ClusterEmulator $1 $2 > nohup.out 2>&1 &
pid=$!
sudo renice -n -20 $pid
