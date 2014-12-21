#!/usr/bin/env python

import os
import sys

lines = os.popen("grep --byte-offset --only-matching --text 'SEQ' %s" % sys.argv[1]).read().split()

i = 0
last_offset = 0

for line in lines[1:]:
	offset = int(line.split(":")[0])
	#command = "dd skip=%s count=%s if=%s of=%s-%s bs=1" % (last_offset, (offset-last_offset), sys.argv[1], sys.argv[2], i)
	command = "dd skip=%s bs=%s if=%s of=%s-%04d count=1 iflag=skip_bytes" % (last_offset, (offset-last_offset), sys.argv[1], sys.argv[2], i)
	print command
	os.popen(command)
	i += 1
	last_offset = offset

