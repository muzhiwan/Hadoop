package com.muzhiwan.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MobileSDKActiveUsersMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	
	private static final String DELIMITER = "\t";
	
	private static final int INDEX_DATE = 0;
	private static final int INDEX_TITLE = 1;
	private static final int INDEX_PACKAGE_NAME = 2;
	private static final int INDEX_VERSION = 3;
	// private static final int INDEX_CELL_PHONE_DEVICE_ID = 4;
	// private static final int INDEX_TIME = 5;
	private static final int INDEX_NETWORK = 6;
	// private static final int INDEX_LOGON_COUNTS = 7;
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String data[] = value.toString().split(DELIMITER);
		
		String mapperKey = String.format("%s\t%s\t%s\t%s\t%s",
				data[INDEX_DATE], data[INDEX_TITLE], data[INDEX_PACKAGE_NAME],
				data[INDEX_VERSION], data[INDEX_NETWORK]);
		String mapperValue = String.format("%d\t%d", 1, 0);
		context.write(new Text(mapperKey), new Text(mapperValue));
	}
}
