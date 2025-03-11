#!/bin/bash

SCRIPT_DIR="$(dirname $(readlink -f $0))"
source "${SCRIPT_DIR}/.venv/bin/activate"

"${SCRIPT_DIR}/test.py" $@
EXIT_CODE=$?

if [[ $EXIT_CODE -eq 0 ]]; then
    echo "Test succeeded."
else
    echo "Test failed with exit code $EXIT_CODE."
fi

exit $EXIT_CODE
