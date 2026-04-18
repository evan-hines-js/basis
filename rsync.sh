#!/bin/bash
rsync -az --delete --exclude='target' --exclude='node_modules' ../basis/ ubuntu@10.0.0.131:~/basis/
