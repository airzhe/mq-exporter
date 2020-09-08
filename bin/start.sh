#!/bin/bash

LOG=../logs

set -e

cd `dirname $0`

mkdir -p ../logs

chmod +x mq-exporter-mon
chmod +x mq-exporter
chmod +x run.sh

nohup ./mq-exporter-mon -d -l  $LOG/mq-exporter-mon.log  ./run.sh &>$LOG/nohup.log