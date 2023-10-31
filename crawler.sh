#!/bin/bash

nohup python3 unocoin/unocoin-v1.py &
nohup python3 coindcx/coindcx-v1.py &
nohup python3 wazirx/wazirx-v1.py &
nohup python3 cryptocompare/cryptocompare_v1.py > cc.txt 2>&1 &