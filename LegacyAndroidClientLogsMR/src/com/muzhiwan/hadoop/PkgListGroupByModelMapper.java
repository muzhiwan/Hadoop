package com.muzhiwan.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PkgListGroupByModelMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	// private static final int INPUT_INDEX_IMEI = 0;
	private static final int INPUT_INDEX_MODEL = 1;
	private static final int INPUT_INDEX_PACKAGENAME = 2;
	private static final int INPUT_INDEX_TITLE = 3;
	private static final int INPUT_INDEX_VERSIONNAME = 4;
	// private static final int INPUT_INDEX_VERSIONCODE = 5;
	// private static final int INPUT_INDEX_TIME = 6;

	@Override
	public void map(LongWritable key, Text value, Context context) {
		String data[] = value.toString().split("\t");
		try {
			String mapperKey = String.format("%s\t%s\t%s\t%s",
					data[INPUT_INDEX_MODEL],
					data[INPUT_INDEX_TITLE],
					data[INPUT_INDEX_PACKAGENAME],
					data[INPUT_INDEX_VERSIONNAME]);
			context.write(new Text(mapperKey), new IntWritable(1));
		} catch (Exception e) {
			System.err.println(value.toString());
			e.printStackTrace();
		}
	}
}
