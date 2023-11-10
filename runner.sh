#!/bin/bash
set -e
python3 ./scripts/consumer.py &
python3 ./scripts/uploader.py &
wait