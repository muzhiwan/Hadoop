package com.muzhiwan.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//10001:开始下载
//10002：下载错误
//10003：下载完成
//10004：下载取消
//10005：获取真实下载路径
//10006：浏览游戏
//10007：用户应用数据
//10008：网盘切换CDN
//10009：卸载游戏

public class LegacyAndroidClientLogsMR {
	private static final String JobName = "testLogsAnalyzer";
	private static final String JobName1 = "getInstalledPkgListGroupByTerminal";
	private static final String JobName2 = "getPopularPkgListGroupByModel";
	
	private static final String SummaryDownloadJob = "getSummaryGameDownloadStatistic2";
	private static final String SummaryPkgDownloadJob = "getSummaryGameDownloadStatisticByPkg2";
	private static final String SummaryDailyPkgDownloadJob = "getDailyGameDownloadStatisticByPkg2";
	
	private static final String DailyDownloadJob = "getDailyDownloadStatistic2";
	
	private static final String ErrorCategoryJob = "getErrorCategory2";
	
	private static final String INPUT_TYPE_POPULAR_GAME_2 = "PopularGame2";
	private static final String INPUT_TYPE_GAME_DOWNLOAD_2 = "GameDownload2";
	private static final String INPUT_TYPE_DAILY_DOWNLOAD_2 = "DailyDownload2";
	private static final String INPUT_TYPE_ERROR_CATEGORY_2 = "ErrorCategory2";
	
	public static final int REDUCE_NUMBER = 6;
	
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err
					.println("Usage: LegacyAndroidClientLogsMR <input type> <input path> <output path>");
			System.exit(-1);
		}
		
		// int exitCode = testLogsAnalyzer(args[0], args[1]);
		// int exitCode = getInstalledPkgListGroupByTerminal(args[0], args[1]);
		// int exitCode = getPkgListGroupByModel(args[0], args[1]);
		
		int exitCode = 0;
		
		if (args[0].equals(INPUT_TYPE_POPULAR_GAME_2)) {
			exitCode = getPopularPkgListGroupByModel2(args[1], args[2]);
		} else if (args[0].equals(INPUT_TYPE_GAME_DOWNLOAD_2)) {
			exitCode = getSummaryGameDownloadStatistic2(args[1], args[2]);
		} else if (args[0].equals(INPUT_TYPE_DAILY_DOWNLOAD_2)) {
			exitCode = getDailyDownloadStatistic2(args[1], args[2]);
		} else if (args[0].equals(INPUT_TYPE_ERROR_CATEGORY_2)) {
			exitCode = getErrorCategory2(args[1], args[2]);
		}

		System.exit(exitCode);
	}
	
	private static int getErrorCategory2(String arg0, String arg1) throws IOException, InterruptedException, ClassNotFoundException {
		long timeStart = System.currentTimeMillis();
		
		Job job = new Job();
		job.setJarByClass(LegacyAndroidClientLogsMR.class);
		job.setJobName(ErrorCategoryJob);
		FileInputFormat.addInputPath(job, new Path(arg0));
		FileOutputFormat.setOutputPath(job, new Path(arg1 + "/ErrorCategory"));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		job.setMapperClass(ErrorCategoryMapper.class);
		job.setNumReduceTasks(REDUCE_NUMBER);
		job.setReducerClass(ErrorCategoryReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		
		long timeEnd = System.currentTimeMillis();
		System.err.println("The total cost: " + (timeEnd - timeStart));
		
		return exitCode;
	}

	private static int getDailyDownloadStatistic2(String arg0, String arg1) throws IOException, InterruptedException, ClassNotFoundException {
		long timeStart = System.currentTimeMillis();
		
		Job job = new Job();
		job.setJarByClass(LegacyAndroidClientLogsMR.class);
		job.setJobName(DailyDownloadJob);
		FileInputFormat.addInputPath(job, new Path(arg0));
		FileOutputFormat.setOutputPath(job, new Path(arg1 + "/DailyDownload"));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		job.setMapperClass(DailyDownloadStatisticMapper.class);
		job.setNumReduceTasks(REDUCE_NUMBER);
		job.setCombinerClass(DailyDownloadStatisticReducer.class);
		job.setReducerClass(DailyDownloadStatisticReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		
		long timeEnd = System.currentTimeMillis();
		System.err.println("The total cost: " + (timeEnd - timeStart));
		
		return exitCode;
	}

	private static int getSummaryGameDownloadStatistic2(String arg0, String arg1) throws Exception {
		long timeStart = System.currentTimeMillis();
		
		int exitCode;
		
		exitCode = getSummaryGameDownloadStatisticByPkg2(arg0, arg1);
		if (exitCode != 0) {
			return exitCode;
		}
		
		Job job = new Job();
		job.setJarByClass(LegacyAndroidClientLogsMR.class);
		job.setJobName(SummaryDownloadJob);
		FileInputFormat.addInputPath(job, new Path(arg1 + "/SummaryDownloadByPkg"));
		FileOutputFormat.setOutputPath(job, new Path(arg1 + "/SummaryDownload"));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		job.setMapperClass(SummaryGameDownloadStatisticMapper.class);
		job.setCombinerClass(SummaryGameDownloadStatisticReducer.class);
		job.setReducerClass(SummaryGameDownloadStatisticReducer.class);
		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		exitCode = job.waitForCompletion(true) ? 0 : 1;
		
		long timeEnd = System.currentTimeMillis();
		System.err.println("The total cost: " + (timeEnd - timeStart));
		
		return exitCode;
	}

	private static int getSummaryGameDownloadStatisticByPkg2(String arg0, String arg1) throws Exception {
		long timeStart = System.currentTimeMillis();
		
		int exitCode;
		
		exitCode = getDailyGameDownloadStatisticByPkg2(arg0, arg1);
		if (exitCode != 0) {
			return exitCode;
		}
		
		Job job = new Job();
		job.setJarByClass(LegacyAndroidClientLogsMR.class);
		job.setJobName(SummaryPkgDownloadJob);
		FileInputFormat.addInputPath(job, new Path(arg1 + "/DailyDownloadByPkg"));
		FileOutputFormat.setOutputPath(job, new Path(arg1 + "/SummaryDownloadByPkg"));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		job.setMapperClass(SummaryGameDownloadStatisticByPkgMapper.class);
		job.setCombinerClass(SummaryGameDownloadStatisticByPkgReducer.class);
		job.setReducerClass(SummaryGameDownloadStatisticByPkgReducer.class);
		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		exitCode = job.waitForCompletion(true) ? 0 : 1;
		
		long timeEnd = System.currentTimeMillis();
		System.err.println("The total cost: " + (timeEnd - timeStart));
		
		return exitCode;
	}

	private static int getDailyGameDownloadStatisticByPkg2(String arg0, String arg1) throws Exception {
		long timeStart = System.currentTimeMillis();
		
		Job job = new Job();
		job.setJarByClass(LegacyAndroidClientLogsMR.class);
		job.setJobName(SummaryDailyPkgDownloadJob);
		FileInputFormat.addInputPath(job, new Path(arg0));
		FileOutputFormat.setOutputPath(job, new Path(arg1 + "/DailyDownloadByPkg"));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		job.setMapperClass(DailyGameDownloadStatisticByPkgMapper.class);
		job.setNumReduceTasks(REDUCE_NUMBER);
		job.setReducerClass(DailyGameDownloadStatisticByPkgReducer.class);
		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(ArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		
		long timeEnd = System.currentTimeMillis();
		System.err.println("The total cost: " + (timeEnd - timeStart));
		
		//if (exitCode == 0) {
		//	exitCode = getPopularPkgListGroupByModel(arg1 + "/Terminal", arg1);
		//}
		
		return exitCode;
	}

	public static int testLogsAnalyzer(String arg0, String arg1) throws Exception {
		long timeStart = System.currentTimeMillis();
		
		Job job = new Job();
		job.setJarByClass(LegacyAndroidClientLogsMR.class);
		job.setJobName(JobName);
		FileInputFormat.addInputPath(job, new Path(arg0));
		FileOutputFormat.setOutputPath(job, new Path(arg1 + "/test"));
		job.setMapperClass(LegacyAndroidClientLogsMapper.class);
		job.setReducerClass(LegacyAndroidClientLogsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		
		long timeEnd = System.currentTimeMillis();
		System.err.println("The total cost: " + (timeEnd - timeStart));
		
		return exitCode;
	}
	
	public static int getInstalledPkgListGroupByTerminal2(String arg0, String arg1) throws Exception {
		long timeStart = System.currentTimeMillis();
		
		Job job = new Job();
		job.setJarByClass(LegacyAndroidClientLogsMR.class);
		job.setJobName(JobName1);
		FileInputFormat.addInputPath(job, new Path(arg0));
		FileOutputFormat.setOutputPath(job, new Path(arg1 + "/Terminal"));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		job.setMapperClass(InstalledPkgListGroupByTerminalMapper.class);
		job.setNumReduceTasks(REDUCE_NUMBER);
		job.setReducerClass(InstalledPkgListGroupByTerminalReducer.class);
		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(ArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		
		long timeEnd = System.currentTimeMillis();
		System.err.println("The total cost: " + (timeEnd - timeStart));
		
		//if (exitCode == 0) {
		//	exitCode = getPopularPkgListGroupByModel(arg1 + "/Terminal", arg1);
		//}
		
		return exitCode;
	}


	public static int getPkgListGroupByModel(String arg0, String arg1)
			throws IOException, InterruptedException, ClassNotFoundException {
		long timeStart = System.currentTimeMillis();
		
		Job job = new Job();
		job.setJarByClass(LegacyAndroidClientLogsMR.class);
		job.setJobName(JobName2);
		FileInputFormat.addInputPath(job, new Path(arg0));
		FileOutputFormat.setOutputPath(job, new Path(arg1 + "/PkgList"));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		job.setMapperClass(PkgListGroupByModelMapper.class);
		job.setNumReduceTasks(REDUCE_NUMBER);
		job.setReducerClass(PkgListGroupByModelReducer.class);
		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		
		long timeEnd = System.currentTimeMillis();
		System.err.println("The total cost: " + (timeEnd - timeStart));

		return exitCode;
	}
	
	public static int getPopularPkgListGroupByModel2(String arg0, String arg1)
			throws Exception {
		long timeStart = System.currentTimeMillis();
		
		int exitCode;
		
		exitCode = getInstalledPkgListGroupByTerminal2(arg0, arg1);
		if (exitCode != 0) {
			return exitCode;
		}
		
		Job job = new Job();
		job.setJarByClass(LegacyAndroidClientLogsMR.class);
		job.setJobName(JobName2);
		FileInputFormat.addInputPath(job, new Path(arg1 + "/Terminal"));
		FileOutputFormat.setOutputPath(job, new Path(arg1 + "/PopularPkgList"));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		job.setMapperClass(PopularPkgListGroupByModelMapper.class);
		job.setNumReduceTasks(REDUCE_NUMBER);
		job.setReducerClass(PopularPkgListGroupByModelReducer.class);
		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		exitCode = job.waitForCompletion(true) ? 0 : 1;
		
		long timeEnd = System.currentTimeMillis();
		System.err.println("The total cost: " + (timeEnd - timeStart));
		
		return exitCode;
	}
}
