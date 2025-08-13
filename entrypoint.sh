#!/bin/bash
set -e
echo "Starting webapp..."
python webapp.py &
echo "Starting processor..."
python processor.py &
wait
echo "All services stopped."

