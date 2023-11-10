#!/bin/bash
set -e
python3 ./consumer.py &
python3 ./uploader.py &
wait
