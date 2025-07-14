#!/bin/bash

# Activate the virtual environment
source ./.venv/bin/activate

# Run your Python script in interactive mode
python3.10 -i src/python/main.py

# Deactivate the virtual environment
deactivate