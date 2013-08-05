package com.muzhiwan.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DailyGameDownloadStatisticByPkgReducer extends
		Reducer<Text, Text, Text, Text> {
	private static final String StartType = "10001";
	private static final String ErrorType = "10002";
	private static final String CompleteType = "10003";
	private static final String CancelType = "10004";
	
	private static final int INDEX_START = 0;
	private static final int INDEX_ERROR = 1;
	private static final int INDEX_COMPLETE = 2;
	private static final int INDEX_CANCEL = 3;
	
	private static final int INDEX_TYPE = 0;
	private static final int INDEX_OPERATION_TAG = 1;
	private static final int INDEX_ERROR_MSG = 2;
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Map<String, Set<String>> errMap = new HashMap<String, Set<String>>();
		int counts[] = {0, 0, 0, 0};
		
		for (Text value : values) {
			String data[] = value.toString().split("\t");
			
			if (data[INDEX_TYPE].equals(StartType)) {
				counts[INDEX_START]++;
			} else if (data[INDEX_TYPE].equals(ErrorType)) {
				if (errMap.containsKey(data[INDEX_OPERATION_TAG])) {
					Set<String> errMsgSet = errMap.get(data[INDEX_OPERATION_TAG]);
					errMsgSet.add(data[INDEX_ERROR_MSG]);
				} else {
					Set<String> errMsgSet = new HashSet<String>();
					errMsgSet.add(data[INDEX_ERROR_MSG]);
					errMap.put(data[INDEX_OPERATION_TAG], errMsgSet);
				}
				
				// counts[INDEX_ERROR]++;
			} else if (data[INDEX_TYPE].equals(CompleteType)) {
				counts[INDEX_COMPLETE]++;
			} else if (data[INDEX_TYPE].equals(CancelType)) {
				counts[INDEX_CANCEL]++;
			}
		}
		
		Iterator<Entry<String, Set<String>>> it = errMap.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Set<String>> entry = it.next();
			counts[INDEX_ERROR] += entry.getValue().size();
		}
		
		context.write(key, new Text(String.format("%d\t%d\t%d\t%d",
				counts[INDEX_START], counts[INDEX_ERROR], counts[INDEX_COMPLETE], counts[INDEX_CANCEL])));
	}
}
