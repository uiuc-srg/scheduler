<?php

/*$schedulerAddress = $argv[1];
$workload = $argv[2];
$scheduler = $argv[3];
$cluster = $argv[4];

//prepare();
deploy();
start();*/

function prepare() {
	global $argv;
	for($i=2; $i<count($argv); $i++) {
		run($argv[$i], "mkdir -p code");
		run($argv[$i], "mkdir -p logs");
		exec("scp target/scheduler-libs.jar $argv[$i]:~/code/");
		exec("scp target/scheduler-1.0*.jar $argv[$i]:~/code/");
	}
}

function kill() {
	global $argv;
	for($i=2; $i<count($argv); $i++) {
		run($argv[$i], "killall java");
	}

}

function deploy() {
	global $argv;
	exec("mvn package");
	for($i=2; $i<count($argv); $i++) {
		exec("scp target/scheduler-1.0*.jar $argv[$i]:~/code/");
	}
	//exec("scp target/scheduler-libs.jar $argv[$i]:~/code/");
	//run($workload, "")
}

function start() {
	//global $workload, $scheduler, $cluster, $schedulerAddress ;
	echo $workload . "\n" . $scheduler . "\n" . $cluster . "\n";
	// start nohup schduler
	run($scheduler, "nohup java -cp code/scheduler-1.0-SNAPSHOT.jar:code/scheduler-libs.jar edu.illinois.cs.srg.scheduler.Scheduler > nohup.out 2>&1 &");
	// sleep - 1
	sleep(1);
	// start nohup cluster
	run($cluster, "nohup java -cp code/scheduler-1.0-SNAPSHOT.jar:code/scheduler-libs.jar edu.illinois.cs.srg.cluster.ClusterEmulator $schedulerAddress > nohup.out 2>&1 &");
	// sleep - wait a lot
	sleep(5);
	// start workload generator
	run($workload, "nohup java -cp code/scheduler-1.0-SNAPSHOT.jar:code/scheduler-libs.jar edu.illinois.cs.srg.workload.WorkloadGenerator $schedulerAddress > nohup.out 2>&1 &");
}

#run("gourav@172.22.138.119", "ls -l");

function run($server, $command) {
	$command = "ssh $server '$command'";
	echo $command . "\n";
	exec($command, $output);
	//print_r($output);
	//echo "\n";
}

?>
