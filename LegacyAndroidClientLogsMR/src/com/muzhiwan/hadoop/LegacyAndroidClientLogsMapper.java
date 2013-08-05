package com.muzhiwan.hadoop;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LegacyAndroidClientLogsMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final String AnalysisType = "10007";
	
	private static final String LineDelimiter = " ";
	private static final String ItemDelimiter = ":";
	
	private static final String TYPE = "type";
	private static final String MODEL = "model";
	private static final String DEFAULTMODEL = "other";
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Map<String, String> data = new HashMap<String, String>();
		
		data = getHashMapFromString(value.toString());
		if (data.containsKey(TYPE)) {
			if (data.get(TYPE).equals(AnalysisType)) {
				String mapperKey = data.get(MODEL);
				if (mapperKey == null) {
					mapperKey = DEFAULTMODEL;
				}
				context.write(new Text(URLDecoder.decode(mapperKey, "UTF-8")), value);
			}
			// context.write(new Text(data.get(TYPE)), new IntWritable(1));
		}
	}

	public static Map<String, String> getHashMapFromString(String string) {
		Map<String, String> result = new HashMap<String, String>();
		
		String line = string.toString();
		String items[] = line.split(LineDelimiter);
		if (items.length > 1) {
			for (String item : items) {
				String itemPair[] = item.split(ItemDelimiter);
				if (itemPair.length == 2) {
					result.put(itemPair[0], itemPair[1]);
				}
			}
		}

		return result;
	}
}
