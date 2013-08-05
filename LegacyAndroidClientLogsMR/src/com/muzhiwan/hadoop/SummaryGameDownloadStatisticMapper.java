package com.muzhiwan.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SummaryGameDownloadStatisticMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	// private static final int INDEX_TITLE = 0;
	// private static final int INDEX_PACKAGE_NAME = 1;
	// private static final int INDEX_VERSION_NAME = 2;
	private static final int INDEX_START = 3;
	private static final int INDEX_ERROR = 4;
	private static final int INDEX_COMPLETE = 5;
	private static final int INDEX_CANCEL = 6;
	
	private static final String MAPPER_KEY = "Total:";
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String data[] = value.toString().split("\t");
		String mapperValue = String.format("%s\t%s\t%s\t%s",
				data[INDEX_START], data[INDEX_ERROR], data[INDEX_COMPLETE], data[INDEX_CANCEL]);
		
		context.write(new Text(MAPPER_KEY), new Text(mapperValue));
	}
}
