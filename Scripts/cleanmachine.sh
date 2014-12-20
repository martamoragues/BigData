#!/bin/bash
for ID in {105..108}
do
	echo "machine arvei-$ID"
	ssh arvei-$ID "rm -rfv hadoop* && rm -rfv testhadoop*"
	#ssh arvei-$ID "ps -f -C ssh | grep \"ssh -N -f\" | awk '{print \$2}' | xargs kill"
done
