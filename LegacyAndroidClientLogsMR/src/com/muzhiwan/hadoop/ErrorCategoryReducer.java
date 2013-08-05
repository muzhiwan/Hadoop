package com.muzhiwan.hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ErrorCategoryReducer extends
		Reducer<Text, Text, Text, IntWritable> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Set<String> opTagSet = new HashSet<String>();
		
		for (Text value : values) {
			opTagSet.add(value.toString());
		}
		
		context.write(key, new IntWritable(opTagSet.size()));
	}
}
