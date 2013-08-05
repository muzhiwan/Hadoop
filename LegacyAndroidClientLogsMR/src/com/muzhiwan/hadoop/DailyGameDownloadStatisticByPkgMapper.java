package com.muzhiwan.hadoop;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DailyGameDownloadStatisticByPkgMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	private static final String StartType = "10001";
	private static final String ErrorType = "10002";
	private static final String CompleteType = "10003";
	private static final String CancelType = "10004";
	
	private static final String TYPE = "type";
	private static final String TIME = "time";
	private static final String TITLE = "title";
	private static final String PACKAGENAME = "packagename";
	private static final String VERSIONNAME = "version";
	private static final String OPERATIONTAG = "operationtag";
	private static final String ERRORMSG = "errormsg";
	
	private static final String DEFAULTTITLE = "title";
	private static final String DEFAULTPACKAGENAME = "packageName";
	private static final String DEFAULTOPERATIONTAG = "operationTag";
	private static final String DEFAULTERRORMESSAGE = "errorMessage";
	private static final String DEFAULTTIME = "0";
	private static final String DEFAULTVERSIONNAME = "0.0.0";
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Map<String, String> data = new HashMap<String, String>();
		
		data = LegacyAndroidClientLogsMapper.getHashMapFromString(value.toString());
		if (data.containsKey(TYPE)) {
			String type = data.get(TYPE);
			if (isValid(type)) {
				String mapperKey = constructKey(data.get(TIME), data.get(TITLE), data.get(PACKAGENAME), data.get(VERSIONNAME));
				
				String opTag = data.get(OPERATIONTAG);
				if (opTag == null) { opTag = DEFAULTOPERATIONTAG; }
				
				String errMsg = data.get(ERRORMSG);
				if (errMsg == null) { errMsg = DEFAULTERRORMESSAGE; }
				
				if (type.equals(ErrorType)) {
					context.write(new Text(mapperKey), new Text(
							String.format("%s\t%s\t%s", type,
									Utility.packWord(opTag),
									Utility.packWord(errMsg))));
				} else {
					context.write(new Text(mapperKey), new Text(type));
				}
				
				/* if (type.equals(StartType)) {
					context.write(new Text(mapperKey), new Text(StartType));
				} else if (type.equals(ErrorType)) {
					context.write(new Text(mapperKey), new Text(ErrorType));
				} */
			}
		}
	}

	private String constructKey(String time, String title, String pkgName, String versionName) throws UnsupportedEncodingException {
		String ret = null;
		
		if (time == null) { time = DEFAULTTIME; }
		if (title == null) { title = DEFAULTTITLE; }
		if (pkgName == null) { pkgName = DEFAULTPACKAGENAME; }
		if (versionName == null) { versionName = DEFAULTVERSIONNAME; }
		
		ret = String.format("%s\t%s\t%s\t%s", Utility.timeToDate(time), Utility.packWord(title), Utility.packWord(pkgName), Utility.packWord(versionName));
		return ret;
	}

	private boolean isValid(String type) {
		return type.equals(StartType) || type.equals(CompleteType) || type.equals(ErrorType) || type.equals(CancelType);
	}
}
