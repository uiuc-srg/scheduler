<?php

include("lib.php");

$experiment = $argv[1];
$schedulerAddress = $argv[2];
$workload = $argv[3];
$scheduler = $argv[4];
$cluster = $argv[5];

deploy();
start();

?>
