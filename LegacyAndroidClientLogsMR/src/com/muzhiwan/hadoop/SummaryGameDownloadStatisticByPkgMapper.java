package com.muzhiwan.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SummaryGameDownloadStatisticByPkgMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	// private static final int INDEX_DATE = 0;
	private static final int INDEX_TITLE = 1;
	private static final int INDEX_PACKAGE_NAME = 2;
	private static final int INDEX_VERSION_NAME = 3;
	private static final int INDEX_START = 4;
	private static final int INDEX_ERROR = 5;
	private static final int INDEX_COMPLETE = 6;
	private static final int INDEX_CANCEL = 7;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String data[] = value.toString().split("\t");
		String mapperKey = String.format("%s\t%s\t%s",
				data[INDEX_TITLE], data[INDEX_PACKAGE_NAME], data[INDEX_VERSION_NAME]);
		String mapperValue = String.format("%s\t%s\t%s\t%s",
				data[INDEX_START], data[INDEX_ERROR], data[INDEX_COMPLETE], data[INDEX_CANCEL]);
		
		context.write(new Text(mapperKey), new Text(mapperValue));
	}
}
