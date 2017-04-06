#!/usr/bin/env bash

# This script was tested using Python 2.7 on Ubuntu 16.04.2 LTS Xenial Xerus
# However, the code was developed on Windows 10 platform using Python 2.7

# Explanation of script 
# python SCRIPT LON_INPUT_FILE FEATURE1_OUTPUT_FILE FEATURE2_OUTPUT_FILE FEATURE3_OUTPUT_FILE FEATURE4_OUTPUT_FILE
# You can write "*" in place of output file name, if you don't want particular feature

python ./src/process_log.py ./log_input/log.txt ./log_output/hosts.txt ./log_output/hours.txt ./log_output/resources.txt ./log_output/blocked.txt

