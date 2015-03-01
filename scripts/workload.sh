#!/bin/bash
nohup java -Xmx70G -cp code/scheduler-1.0-SNAPSHOT.jar:code/scheduler-libs.jar edu.illinois.cs.srg.workload.WorkloadGenerator $1 $2 $3 $4 $5 $6 > nohup.out 2>&1 &
pid=$!
sudo renice -n -19 $pid
