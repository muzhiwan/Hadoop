package com.muzhiwan.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PopularPkgListGroupByModelReducer extends
		Reducer<Text, Text, Text, Text> {
	// private static final int INPUT_INDEX_IMEI = 0;
	// private static final int INPUT_INDEX_MODEL = 1;
	private static final int INPUT_INDEX_PACKAGENAME = 2;
	private static final int INPUT_INDEX_TITLE = 3;
	private static final int INPUT_INDEX_VERSIONNAME = 4;
	// private static final int INPUT_INDEX_VERSIONCODE = 5;
	// private static final int INPUT_INDEX_TIME = 6;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Map<String, Integer> pkgMap = new HashMap<String, Integer>();
		
		for (Text value : values) {
			String data[] = value.toString().split("\t");
			String reducerKey = String.format("%s\t%s\t%s",
					data[INPUT_INDEX_TITLE],
					data[INPUT_INDEX_PACKAGENAME],
					data[INPUT_INDEX_VERSIONNAME]);
			
			if (pkgMap.containsKey(reducerKey)) {
				pkgMap.put(reducerKey, pkgMap.get(reducerKey) + 1);
			} else {
				pkgMap.put(reducerKey, 1);
			}
		}
		
		ArrayList<String[]> pkgList = new ArrayList<String[]>();
		Iterator<Entry<String, Integer>> it = pkgMap.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Integer> entry = it.next();
			String strArrEntry[] = {entry.getKey(), entry.getValue().toString()};
			pkgList.add(strArrEntry);
		}
		
		Collections.sort(pkgList, this.new PackageComparator());
		
		for (int i = 0; i < pkgList.size(); i++) {
			context.write(new Text(String.format("%s\t%s", key.toString(), pkgList.get(i)[0])), new Text(pkgList.get(i)[1]));
		}
	}
	
	class PackageComparator implements Comparator<String[]> {

		@Override
		public int compare(String[] o1, String[] o2) {
			return Integer.parseInt(o2[1]) - Integer.parseInt(o1[1]);
		}

	}
}
