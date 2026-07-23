# trying a smaller awk script because the other one was too much?
BEGIN {
	# call with --csv
	#FS=","
	
}
{
	if (NR > 1) {
		# count total hours
		hours += $7
	}
	
}
END {
	print "total hours = ", hours
	print "total rows = ", (NR - 1)
}
