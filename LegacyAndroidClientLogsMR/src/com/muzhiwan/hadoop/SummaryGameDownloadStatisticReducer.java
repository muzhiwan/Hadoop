package com.muzhiwan.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SummaryGameDownloadStatisticReducer extends
		Reducer<Text, Text, Text, Text> {
	private static final int INDEX_START = 0;
	private static final int INDEX_ERROR = 1;
	private static final int INDEX_COMPLETE = 2;
	private static final int INDEX_CANCEL = 3;
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int counts[] = {0, 0, 0, 0};
		
		for (Text value : values) {
			String data[] = value.toString().split("\t");
			counts[INDEX_START] += Integer.parseInt(data[INDEX_START]);
			counts[INDEX_ERROR] += Integer.parseInt(data[INDEX_ERROR]);
			counts[INDEX_COMPLETE] += Integer.parseInt(data[INDEX_COMPLETE]);
			counts[INDEX_CANCEL] += Integer.parseInt(data[INDEX_CANCEL]);
		}
		
		context.write(key, new Text(String.format("%d\t%d\t%d\t%d",
				counts[INDEX_START], counts[INDEX_ERROR], counts[INDEX_COMPLETE], counts[INDEX_CANCEL])));
	}
}
