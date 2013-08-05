package com.muzhiwan.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DailyDownloadStatisticMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private static final String TYPE = "type";
	private static final String TIME = "time";
	
	private static final String StartType = "10001";
	
	private static final String DEFAULTTIME = "0";
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Map<String, String> data = new HashMap<String, String>();
		
		data = LegacyAndroidClientLogsMapper.getHashMapFromString(value.toString());
		if (data.containsKey(TYPE)) {
			String type = data.get(TYPE);
			if (type.equals(StartType)) {
				String time = data.get(TIME);
				if (time == null) { time = DEFAULTTIME; }
				
				context.write(new Text(Utility.timeToDate(time)), new IntWritable(1));
			}
		}
	}
}
