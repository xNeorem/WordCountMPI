#!/bin/bash

MAX=10
MAIN = wordcount
OUT_FOLDER = ./out/

for (( i=1; i <= $MAX; ++i ))
do
    mpirun --allow-run-as-root -np $i $OUT_FOLDER+$MAIN
done