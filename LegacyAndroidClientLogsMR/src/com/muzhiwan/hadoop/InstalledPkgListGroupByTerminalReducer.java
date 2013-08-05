package com.muzhiwan.hadoop;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InstalledPkgListGroupByTerminalReducer extends
		Reducer<Text, Text, Text, Text> {
	private static final int INPUT_INDEX_VERSIONNAME = 0;
	private static final int INPUT_INDEX_VERSIONCODE = 1;
	private static final int INPUT_INDEX_TIME = 2;
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		PkgEntry pkgEntry = new PkgEntry();

		for (Text value : values) {
			// VERSIONNAME\tVERSIONCODE\tTIME
			// {verName, verCode, time}
			String pkgInfo[] = value.toString().split("\t");
			pkgEntry.put(pkgInfo[INPUT_INDEX_VERSIONNAME],
					pkgInfo[INPUT_INDEX_VERSIONCODE],
					pkgInfo[INPUT_INDEX_TIME]);
			
		}
		
		context.write(key, new Text(pkgEntry.toString()));
	}
	
	class PkgEntry {
		private String verName;
		private String verCode;
		private String time;
		
		public PkgEntry() {
			verName = null;
			verCode = null;
			time = null;
		}
		
		public void put(String versionName, String versionCode, String time)
				throws UnsupportedEncodingException {
			if (verName == null || Long.parseLong(this.time) < Long.parseLong(time)) {
				verName = Utility.packWord(versionName);
				verCode = Utility.packWord(versionCode);
				this.time = Utility.packWord(time);
			}
		}
		
		public String toString() {
			return String.format("%s\t%s\t%s", verName, verCode, time);
		}
	}
}
