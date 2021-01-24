#!/bin/bash
set -e
cd `dirname $0`

CONFIG=../config/config.yaml
BIN=./mq-exporter
LOG=../logs
chmod +x $BIN
$BIN -config=$CONFIG -consul=$1 -secret=XMUONPHR4G2WVLBR  &>$LOG/nohup.log
