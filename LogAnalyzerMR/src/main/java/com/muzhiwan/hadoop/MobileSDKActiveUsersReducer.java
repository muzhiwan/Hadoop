package com.muzhiwan.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MobileSDKActiveUsersReducer extends
		Reducer<Text, Text, Text, Text> {
	private static final String DELIMITER = "\t";
	
/*	private static final int INDEX_DATE = 0;
	private static final int INDEX_TITLE = 1;
	private static final int INDEX_PACKAGE_NAME = 2;
	private static final int INDEX_VERSION = 3;
	private static final int INDEX_CELL_PHONE_DEVICE_ID = 4;
	private static final int INDEX_NETWORK = 5;*/
	
	private static final int INDEX_ACTIVE_USER_COUNTS = 0;
	private static final int INDEX_NEW_USER_COUNTS = 1;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int activeUserCounts = 0;
		int newUserCounts = 0;
		
		for (Text value : values) {
			String valueArray[] = value.toString().split(DELIMITER);
			activeUserCounts += Integer.parseInt(valueArray[INDEX_ACTIVE_USER_COUNTS]);
			newUserCounts += Integer.parseInt(valueArray[INDEX_NEW_USER_COUNTS]);
		}
		
		context.write(key, new Text(String.format("%d\t%d", activeUserCounts, newUserCounts)));
	}
}
