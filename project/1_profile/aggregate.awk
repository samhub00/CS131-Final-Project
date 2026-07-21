BEGIN {
	# call with --csv
	#FS=","
	
}
{
	if (NR > 1) {
		# count unique users
		users[$4]++
		# count total playtime
		hours += $7
		# count playtime in last 2 weeks
		recenthours += $8
		# count languages
		languages[$11]++
	}
	
}
END {
	#for (u in users) {
	#	print u, " wrote ", users[u], " reviews"
	#}
	print "total row count = ", (NR - 1)
	print "unique users = ", length(users)
	print "total playtime sum = ", hours
	print "average total playtime = ", (hours / (NR - 1))
	print "total playtime in last 2 weeks sum = ", recenthours
	print "average total playtime in last 2 weeks = ", (recenthours / (NR - 1))
	print "\nlanguage counts:"
	for (l in languages) {
		print "\t", l, " = ", languages[l]
	}
}
