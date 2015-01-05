#!/usr/bin/env php
<?php
$datos = array();


$jobtracker = file_get_contents($argv[1].'/jobtracker.jsp.html');

preg_match('|Identifier:.*?(\d+)|s', $jobtracker, $identifier);
$identifier = $identifier[1];

$jobhistoryhome = file_get_contents($argv[1].'/jobhistoryhome.jsp?pageno=-1&search=&scansize=0.html');
if(preg_match_all('|href="(jobdetailshistory[^"]*)">job_'.$identifier.'|', $jobhistoryhome, $urls)){
	$urls = $urls[1];
	for ($i = (count($urls)-1); $i>=0; $i--) {
		$jobdetails = urldecode(html_entity_decode($urls[$i]));
		$jobdetailshistory = file_get_contents($argv[1].'/'.$jobdetails);

		preg_match('|<b>JobName:\s*</b>\s*(.*?)\s*<br/>|s', $jobdetailshistory, $jobname);
		$jobname = $jobname[1];

		preg_match('|<b>Finished At:(.*?)<br/>|s', $jobdetailshistory, $html_total);
		$total = takeTime($html_total[1]);

		preg_match('|<td>Map</td>(.*?)<td>Reduce</td>|s', $jobdetailshistory, $html_map);
		$map = takeTime($html_map[1]);

		preg_match('|<td>Reduce</td>(.*?)<td>Cleanup</td>|s', $jobdetailshistory, $html_reduce);
		$reduce = takeTime($html_reduce[1]);

		print "$jobname\t$total\t$map\t$reduce\n";
	}
}


function takeTime ($singleTaskHtml){
    // if (preg_match('|<td>\d{1,2}-[a-z]{3}-\d{4}\s*\d{1,2}:\d{2}:\d{2}\s*\((\d*)[mins, ]*(\d*)sec\)|is', $singleTaskHtml, $time)){
    if (preg_match('|\((\d*)[mins, ]*(\d*)sec\)|is', $singleTaskHtml, $time)){
    	if($time[2] !== "") {
        	return $time[1] . "\t" . $time[2];
    	} else {
			return "0" . "\t" . $time[1];
    	}
    }
}


