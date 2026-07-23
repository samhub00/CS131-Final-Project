# trying a smaller awk script because the other one was too much?
BEGIN {
	# call with --csv
	#FS=","
	
}
{
	if (NR > 1) {
		# count unique users
		users[$4]++
	}
	
}
END {
	#for (u in users) {
	#	print u, " wrote ", users[u], " reviews"
	#}
	print "unique users = ", length(users)
}
