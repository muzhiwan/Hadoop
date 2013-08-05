package com.muzhiwan.hadoop;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LegacyAndroidClientLogsReducer extends
		Reducer<Text, Text, Text, Text> {
	private static final String MODEL = "model";
	private static final String IMEI = "imei";
	private static final String PACKAGENAME = "packageName";
	private static final String DEFAULTPACKAGENAME = "packageName";
	private static final String VERSIONNAME = "versionName";
	private static final String DEFAULTVERSIONNAME = "versionName";
	private static final String TITLE = "title";
	private static final String DEFAULTTITLE = "title";
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Set<String> cellPhoneSet = new HashSet<String>();
		Map<String, GamePackage> gameMap = new HashMap<String, GamePackage>();
		Map<String, String> data = new HashMap<String, String>();
		
		for (Text value : values) {
			data = LegacyAndroidClientLogsMapper.getHashMapFromString(value.toString());
			
			cellPhoneSet.add(data.get(IMEI));
			
			String packageName = data.get(PACKAGENAME);
			if (packageName == null) { packageName = DEFAULTPACKAGENAME; }
			
			String versionName = data.get(VERSIONNAME);
			if (versionName == null) { versionName = DEFAULTVERSIONNAME; }
			
			String gameMapKey = String.format("%s%s", packageName, versionName);
			if (gameMap.containsKey(gameMapKey)) {
				gameMap.get(gameMapKey).incCount();
			} else {
				String title = data.get(TITLE);
				if (title == null) { title = DEFAULTTITLE; }
				
				GamePackage gamePackage = new GamePackage(
						URLDecoder.decode(packageName, "UTF-8"),
						URLDecoder.decode(title, "UTF-8"),
						URLDecoder.decode(versionName, "UTF-8"), 1);
				gameMap.put(gameMapKey, gamePackage);
			}

			// context.write(new Text(data.get(TYPE)), new IntWritable(1));
		}
		
		// if (cellPhoneSet.size() > 100) { context.write(key, outputGameMap(gameMap)); }
		context.write(key, outputGameMap(gameMap));
		
		System.err.println("Model (" + data.get(MODEL) + ") has " + cellPhoneSet.size() + " terminals.");
		// context.write(key, new IntWritable(count));
	}
	
	public Text outputGameMap(Map<String, GamePackage> gameMap) {
		GamePackage gamePackages[] = gameMap.values().toArray(new GamePackage[0]);
		Arrays.sort(gamePackages, this.new GamePackageComparator());
		return new Text(Arrays.toString(gamePackages));
	}

	class GamePackage {
		private String packageName;
		private String title;
		private String versionName;
		private int count;
		
		@SuppressWarnings("unused")
		private GamePackage() {}
		
		public GamePackage(String packageName, String title, String versionName, int count) {
			this.packageName = packageName;
			this.title = title;
			this.versionName = versionName;
			this.count = count;
		}
		
		public String getPackageName() { return packageName; }
		public String getTitle() { return title; }
		public String getVersionName() { return versionName; }
		public int getCount() { return count; }
		
		public int incCount() { return ++count; }
		public String toString() {
			return String.format("%s\t%s\t%d\t%s\n", title, versionName, count, packageName);
		}
	}
	
	class GamePackageComparator implements Comparator<GamePackage> {

		@Override
		public int compare(GamePackage o1, GamePackage o2) {
			return o2.getCount() - o1.getCount();
		}

	}
}
