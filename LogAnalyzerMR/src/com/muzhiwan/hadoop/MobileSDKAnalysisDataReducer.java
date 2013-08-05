package com.muzhiwan.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MobileSDKAnalysisDataReducer extends
		Reducer<Text, Text, Text, Text> {
	private static final String DELIMITER = "\t";
	
/*	private static final int INDEX_DATE = 0;
	private static final int INDEX_TITLE = 1;
	private static final int INDEX_PACKAGE_NAME = 2;
	private static final int INDEX_VERSION = 3;
	private static final int INDEX_CELL_PHONE_DEVICE_ID = 4;*/
	
	private static final int INDEX_TIME = 0;
	private static final int INDEX_NETWORK = 1;
	private static final int INDEX_LOGON_COUNTS = 2;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int logonCounts = 0;
		long lFirstTime = 0L;
		String network = null;
		
		for (Text value : values) {
			String valueArray[] = value.toString().split(DELIMITER);
			
			long lTime = Long.parseLong(valueArray[INDEX_TIME]);
			if (lFirstTime == 0 || lFirstTime > lTime) {
				lFirstTime = lTime;
				network = valueArray[INDEX_NETWORK];
			}
			
			logonCounts += Integer.parseInt(valueArray[INDEX_LOGON_COUNTS]);
		}
		
		context.write(key, new Text(String.format("%d\t%s\t%d", lFirstTime, network, logonCounts)));
	}
}
