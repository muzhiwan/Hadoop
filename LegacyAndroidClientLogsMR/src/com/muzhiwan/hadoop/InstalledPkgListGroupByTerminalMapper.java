package com.muzhiwan.hadoop;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InstalledPkgListGroupByTerminalMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	private static final String AnalysisType = "10007";
	
	private static final String TYPE = "type";
	private static final String IMEI = "imei";
	private static final String DEFAULTIMEI = "AAAAAAAA";
	
	private static final String MODEL = "model";
	private static final String DEFAULTMODEL = "other";
	
	private static final String PACKAGENAME = "packageName";
	private static final String DEFAULTPACKAGENAME = "packageName";
	
	private static final String VERSIONNAME = "versionName";
	private static final String DEFAULTVERSIONNAME = "versionName";
	
	private static final String VERSIONCODE = "versionCode";
	private static final String DEFAULTVERSIONCODE = "0";
	
	private static final String TITLE = "title";
	private static final String DEFAULTTITLE = "title";
	
	private static final String TIME = "time";
	private static final String DEFAULTTIME = "0";
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Map<String, String> data = new HashMap<String, String>();
		
		data = LegacyAndroidClientLogsMapper.getHashMapFromString(value.toString());
		if (data.containsKey(TYPE)) {
			if (data.get(TYPE).equals(AnalysisType)) {
				// Key: IMEI\tMODEL\tPACKAGENAME\tTITLE
				String mapperKey = constructKey(data.get(IMEI), data.get(MODEL), data.get(PACKAGENAME), data.get(TITLE));
				try {
					// Value: VERSIONNAME\tVERSIONCODE\tTIME
					context.write(new Text(mapperKey), new Text( constructDeviceInfoArray(
							data.get(VERSIONNAME),
							data.get(VERSIONCODE),
							data.get(TIME)) ));
				} catch(NullPointerException e) {
					System.err.println(String.format("%s %s %s %s %s %s", data.toString(), data.get(MODEL),
							data.get(PACKAGENAME),
							data.get(TITLE),
							data.get(VERSIONNAME),
							data.get(VERSIONCODE),
							data.get(TIME)));
					System.err.println(value.toString());

					// throw(e);
				}
			}
		}
	}

	private String constructKey(String imei, String model, String pkgName, String title)
			throws UnsupportedEncodingException {
		if (imei == null) { imei = DEFAULTIMEI; }
		if (model == null) { model = DEFAULTMODEL; }
		if (pkgName == null) { pkgName = DEFAULTPACKAGENAME; }
		if (title == null) { title = DEFAULTTITLE; }
		
		return String.format("%s\t%s\t%s\t%s", Utility.packWord(imei), Utility.packWord(model), Utility.packWord(pkgName), Utility.packWord(title));
	}

	private String constructDeviceInfoArray(String verName, String verCode,
			String time) throws UnsupportedEncodingException {
		if (verName == null) { /* System.out.println("verName is null"); */ verName = DEFAULTVERSIONNAME; }
		if (verCode == null) { /* System.out.println("verCode is null"); */ verCode = DEFAULTVERSIONCODE; }
		if (time == null) { /* System.out.println("verCode is null"); */ time = DEFAULTTIME; }
		
		String ret = String.format("%s\t%s\t%s", Utility.packWord(verName),
				Utility.packWord(verCode), Utility.packWord(time));

		//System.err.println(ret);
		return ret;
	}
}
