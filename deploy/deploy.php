<?php

include("lib.php");

$schedulerAddress = $argv[1];
$workload = $argv[2];
$scheduler = $argv[3];
$cluster = $argv[4];

deploy();
start();

?>
