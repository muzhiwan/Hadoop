package com.muzhiwan.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PopularPkgListGroupByModelMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	// private static final int INPUT_INDEX_IMEI = 0;
	private static final int INPUT_INDEX_MODEL = 1;
	// private static final int INPUT_INDEX_PACKAGENAME = 2;
	// private static final int INPUT_INDEX_TITLE = 3;
	// private static final int INPUT_INDEX_VERSIONNAME = 4;
	// private static final int INPUT_INDEX_VERSIONCODE = 5;
	// private static final int INPUT_INDEX_TIME = 6;

	@Override
	public void map(LongWritable key, Text value, Context context) {
		String data[] = value.toString().split("\t");
		try {
			context.write(new Text(data[INPUT_INDEX_MODEL]), value);
		} catch (Exception e) {
			System.err.println(value.toString());
			e.printStackTrace();
		}
	}
}
