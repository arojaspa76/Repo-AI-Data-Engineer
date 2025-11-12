#!/bin/bash

# Activate the Python 3.12 virtual environment
VENV_PATH="/media/arojaspa/Data/dev/environments/ubuntu/cursoetl_tf_ubuntu"

if [ -f "$VENV_PATH/bin/activate" ]; then
    echo "Activating virtual environment at: $VENV_PATH"
    source "$VENV_PATH/bin/activate"
    echo "Virtual environment activated."
else
    echo "ERROR: Virtual environment not found at $VENV_PATH"
    exit 1
fi