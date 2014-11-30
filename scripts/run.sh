#!/bin/bash
mvn package
php deploy/start.php test 172.22.138.85 scheduler@172.22.138.84 gourav@172.22.138.85 scheduler@172.22.138.87
