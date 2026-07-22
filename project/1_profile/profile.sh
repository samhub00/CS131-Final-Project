#!/bin/bash
# Shell script to run all of our commands in for profiling easily
# Run the following command to get an output file:
# bash profile.sh ../all_reviews.csv 2>&1 | tee results.txt

# get file from commandline arg
INPUTFILE=$1

# test if file exists and print some lines
if [[ ! -f $INPUTFILE ]]; then
	# file isn't correct, output error message
	echo "Error: $INPUTFILE is not a file"
else
	echo "Running profiling on input file $INPUTFILE"
	echo "---"

	# print filesize and how many rows
	echo "--- FILE SIZE ---"
	echo "Checking filesize..."
	echo "Filesize: $(time du -h $INPUTFILE)"
	echo ""
	# edited out because row count is checked in awk
	#echo "Checking row count... (takes a while)"
	#echo "Row count: $(time wc -l $INPUTFILE)"
	
	# schema peek
	echo "--- SCHEMA ---"
	echo "Getting column headers..."
	echo "Column headers: $(time head -1 $INPUTFILE)"
	#echo "Column count: $(time head -1 $INPUTFILE | wc)"
	echo "Table snippet:"
	echo "$(time head -6 $INPUTFILE | column -t -s ',')"
	echo ""
	#TODO somehow use cut?

	# top categories & cardinality
	echo "--- "
	#echo "$(time head -100 $INPUTFILE | sort | uniq -c | sort -nr)"
	
	# awk, basic aggregate of some columns
	echo "--- AGGREGATE ---"
	echo "Running awk on file... (will take a long time)"
	# note: must call awk with --csv or it breaks at around line ~234150 due to commas
	# TODO call with whole file instead? was taking way too long for a test run
	echo "$(time head -100000000 $INPUTFILE | awk --csv -f aggregate.awk )"

fi


