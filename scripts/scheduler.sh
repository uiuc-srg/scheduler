#!/bin/bash
nohup java -Xmx70G -cp code/scheduler-1.0-SNAPSHOT.jar:code/scheduler-libs.jar edu.illinois.cs.srg.scheduler.Scheduler $1 > nohup.out 2>&1 &
pid=$!
sudo renice -n -20 $pid
