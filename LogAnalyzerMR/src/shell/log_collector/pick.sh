#!/bin/bash

events=(100001 100002 100003 100004 200001 200002 200003 200004 200005 200006 200007 200008 200009) 
month="08"
days=(01 02 03 04 05 06 07 08)

for event in "${events[@]}"; do
	for day in "${days[@]}"; do
		/root/logs/log_collector.sh "${event}" "${month}" "${day}"
	done
done

