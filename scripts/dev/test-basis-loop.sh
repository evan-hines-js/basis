#!/bin/bash
run=1
while true; do
  echo "=== Run $run started at $(date) ==="
  bash scripts/dev/test-basis.sh
  rc=$?
  if [ $rc -ne 0 ]; then
    echo "=== FAILED on run $run at $(date) (exit code $rc) ==="
    exit $rc
  fi
  echo "=== Run $run passed at $(date) ==="
  ((run++))
done
