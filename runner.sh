#!/bin/bash
set -e
python3 ./consumer.py &
python3 ./uploader.py &
while true; do cat /dev/null > consumer.log; sleep 1; done; &
