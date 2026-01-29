#!/bin/bash

# Run the script using uv (handles environment and python version automatically)
cd /home/eduardo/documents/money_manager
uv run -m money_manager.main

# Wait for user input
echo ""
echo "Process finished. Press Enter to close this window..."
read
