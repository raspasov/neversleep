#!/usr/bin/env bash
. /etc/neversleep-db/jvm-config-original
. /etc/neversleep-db/jvm-config

nohup java $JVM_OPTIONS -jar /usr/bin/neversleep-db/neversleep-db.jar "/etc/neversleep-db/config.clj" &
echo 0;