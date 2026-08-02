#!/bin/bash
# scaling_full.sh - run the same Spark job on 1 worker, then 2, then 4, record each runtime
# code mainly taken from Spark demo 2 page

# fail fast
set -euo pipefail

BUCKET=gs://cs131-group-project-kyle-sam
REGION=us-central1

INPUTFILE=$BUCKET/data/all_reviews.csv
SCRIPT=$BUCKET/code/scaling.py
OUTPUTDEST=$BUCKET/output/scaling

for N in 2 4 6; do
    echo "=== Submitting batch with $N executor(s) ==="
    gcloud dataproc batches submit pyspark $SCRIPT \
        --region=$REGION \
        --deps-bucket=$BUCKET \
        --properties=spark.dynamicAllocation.enabled=false,spark.executor.instances="$N" \
        -- $INPUTFILE $OUTPUTDEST$N 2>&1 | tee results_scaling_$N.txt
    
    # wait between runs so the finished batch's machines release before the next one
    if [ "$N" -ne 6 ]; then
        echo "=== Waiting 8 minutes for machines to release ==="
        sleep 480
    fi
done

echo "=== All runs done. Compute times are in their respective output files ==="