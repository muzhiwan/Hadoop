package com.muzhiwan.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ErrorCategoryMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	private static final String TYPE = "type";
	private static final String ERROR_MSG = "errormsg";
	private static final String ERROR_CODE = "errorcode";
	private static final String OPERATIONTAG = "operationtag";
	
	private static final String ErrorType = "10002";
	private static final String DEFAULT_ERROR_MSG = "NULL";
	private static final String DEFAULT_ERROR_CODE = "0";
	private static final String DEFAULTOPERATIONTAG = "operationTag";
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Map<String, String> data = new HashMap<String, String>();
		
		data = LegacyAndroidClientLogsMapper.getHashMapFromString(value.toString());
		if (data.containsKey(TYPE)) {
			String type = data.get(TYPE);
			if (type.equals(ErrorType)) {
				String errorMsg = data.get(ERROR_MSG);
				if (errorMsg == null) { errorMsg = DEFAULT_ERROR_MSG; }
				String errorCode = data.get(ERROR_CODE);
				if (errorCode == null) { errorCode = DEFAULT_ERROR_CODE; }
				
				String mapperKey = String.format("%s %s", errorCode, Utility.packWord(errorMsg));
				
				String opTag = data.get(OPERATIONTAG);
				if (opTag == null) { opTag = DEFAULTOPERATIONTAG; }
				
				context.write(new Text(mapperKey), new Text(Utility.packWord(opTag)));
			}
		}
	}
}
