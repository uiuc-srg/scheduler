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
	for($i=1; $i<count($argv); $i++) {
		run($argv[$i], "mkdir -p code");
		run($argv[$i], "mkdir -p logs");
		run($argv[$i], "mkdir -p scripts");
		exec("scp target/scheduler-libs.jar $argv[$i]:~/code/");
		exec("scp target/scheduler-1.0*.jar $argv[$i]:~/code/");
		exec("scp scripts/* $argv[$i]:~/scripts/");
	}
}

function kill() {
	global $argv;
	for($i=1; $i<count($argv); $i++) {
		run($argv[$i], "killall java");
	}
}

function deploy() {
	echo "deploying\n";
	global $argv;
	//$array = array($workload, $scheduler, $cluster);
	//exec("mvn package");
	//foreach($array as $machine) {
	for($i=1; $i<count($argv); $i++) {
		$machine = $argv[$i];
		echo "scp target/scheduler-1.0*.jar $machine:~/code/\n";
		exec("scp target/scheduler-1.0*.jar $machine:~/code/");
		exec("scp scripts/* $machine:~/scripts/");
	}
	//exec("scp target/scheduler-libs.jar $argv[$i]:~/code/");
	//run($workload, "")
}

function start() {
	global $workload, $scheduler, $cluster, $schedulerAddress, $experiment ;
	//echo $workload . "\n" . $scheduler . "\n" . $cluster . "\n";
	// start nohup schduler
	runSudo($scheduler, "sh scripts/scheduler.sh");
	// sleep - 1
	sleep(1);
	// start nohup cluster
	runSudo($cluster, "sh scripts/cluster.sh $experiment $schedulerAddress");
	// sleep - wait a lot
	sleep(1);
	// start workload generator
	runSudo($workload, "sh scripts/workload.sh $experiment $schedulerAddress");
}

#run("gourav@172.22.138.119", "ls -l");

function run($server, $command) {
	$command = "ssh $server '$command'";
	echo $command . "\n";
	exec($command, $output);
	//print_r($output);
	//echo "\n";
}

function runSudo($server, $command) {
	$command = "ssh -t $server '$command'";
	echo $command . "\n";
	exec($command, $output);
	//print_r($output);
	//echo "\n";
}

?>
