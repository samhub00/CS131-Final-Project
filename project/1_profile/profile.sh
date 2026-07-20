#!/bin/bash
# Shell script to run all of our commands in for profiling easily
# Run the following command to get an output file:
# bash profile.sh file.csv > tee results.txt

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
	#echo "Checking row count..."
	#echo "Row count: $(time wc -l $INPUTFILE)"
	
	# schema peek
	echo "--- SCHEMA ---"
	echo "Getting column headers..."
	echo "Column headers: $(time head -1 $INPUTFILE)"
	echo "Column count: $(time head -1 $INPUTFILE | wc)"
	echo "Table snippet:"
	echo "$(time head -6 $INPUTFILE | column -t -s ',')"
	echo ""
	#TODO somehow use cut?


	echo ""


fi


# how big is the file? how many rows?
