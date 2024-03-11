#!/bin/bash

nohup python3 unocoin/unocoin-v1.py > unocoin.txt 2>&1 &
nohup python3 coindcx/coindcx-v1.py > coindcx.txt 2>&1 &
nohup python3 wazirX/wazirx-v1.py > wazirx.txt 2>&1 &
# nohup python3 cryptocompare/cryptocompare_v1.py > cc.txt 2>&1 &