package com.muzhiwan.hadoop;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.muzhiwan.hadoop.MobileGameUserInfoMR.GameUserInfoMapper;
import com.muzhiwan.hadoop.MobileGameUserInfoMR.GameUserInfoReducer;
import com.muzhiwan.hadoop.MobileSDKActiveUserMR.ActiveUserMapper;
import com.muzhiwan.hadoop.MobileSDKActiveUserMR.ActiveUserReducer;
import com.muzhiwan.hadoop.MobileSDKUserInfoMR.MobileSDKUserInfoMapper;
import com.muzhiwan.hadoop.MobileSDKUserInfoMR.MobileSDKUserInfoReducer;

/*
 *  
 ������� 200001;
 ���ش��� 200002;
 ������� 200003;
 ����ȡ�� 200004;
 ���ؿ�ʼ 200005;
 �����Ϸ 200006;
 �û���װ���� 200007;
 �л�����URL 200008;
 �û�ж��Ӧ�� 200009;
 */

public class JobRunner {
	private static final String TYPE_MOBILE_GAME_DOWNLOAD = "MobileGameDownload";
	private static final String TYPE_MOBILE_SDK_ACTIVE_USER = "MobileSDKActiveUser";
//	private static final String TYPE_MOBILE_SDK_ANALYSIS = "MobileSDKAnalysis";
	private static final String TYPE_MOBILE_SDK_USERINFO = "MobileSDKUserInfo";
	private static final String TYPE_MOBILE_GAME_USERINFO = "MobileGAMEUserInfo";

	private static final String JOB_MOBILE_GAME_DOWNLOAD = "Mobile Game Download Statistic";
	private static final String JOB_MOBILE_SDK_ACTIVE_USER = "Mobile SDK Active User Statistic";
//	private static final String JOB_MOBILE_SDK_ANALYSIS = "Mobile SDK Analysis Data Collection";
	private static final String JOB_MOBILE_SDK_USERINFO = "Mobile SDK Analysis User Info Collection";
	private static final String JOB_MOBILE_GAME_USERINFO = "Mobile GAME Analysis User Info Collection";

	// private static final int REDUCE_NUMBER = 6;

	private static Map<String, Method> jobEntryMap = new HashMap<String, Method>();
	static {
		try {
			jobEntryMap.put(TYPE_MOBILE_GAME_DOWNLOAD, JobRunner.class
					.getMethod("getMobileGameDownloadStatistic", String.class,
							String.class));
			jobEntryMap.put(TYPE_MOBILE_SDK_ACTIVE_USER, JobRunner.class
					.getMethod("getMobileSDKActiveUsers", String.class,
							String.class));
			jobEntryMap.put(TYPE_MOBILE_SDK_USERINFO, JobRunner.class
					.getMethod("getMobileSDKUserInfo", String.class,
							String.class));
			jobEntryMap.put(TYPE_MOBILE_GAME_USERINFO, JobRunner.class
					.getMethod("getMobileGameUserInfo", String.class,
							String.class));
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// System.err.println("<input type> <input path> <output path>: <%s> <%s> <%s>",
			// );
			e.printStackTrace();
		}
	}

	public static int executeAnalysis(String type, String inputPath,
			String outputPath) throws IllegalArgumentException,
			IllegalAccessException, InvocationTargetException {

		if (jobEntryMap.containsKey(type)) {
			return (Integer) jobEntryMap.get(type).invoke(null, inputPath,
					outputPath);
		}
		return 0;
	}

//	public static int getMobileSDKActiveUsers(String inputPath,
//			String outputPath) throws IOException, InterruptedException,
//			ClassNotFoundException {
//		long timeStart = System.currentTimeMillis();
//		int exitCode;
//
//		String analysisPath = String.format("%s/%s/%s", outputPath,
//				TYPE_MOBILE_SDK_ANALYSIS, Utility.timeToDate(timeStart));
//
//		exitCode = getMobileSDKAnalysisData(inputPath, analysisPath);
//		if (exitCode != 0) {
//			return exitCode;
//		}
//
//		Job job = new Job();
//		job.setJarByClass(LogAnalyzerMR.class);
//		job.setJobName(JOB_MOBILE_SDK_ACTIVE_USER);
//		FileInputFormat.addInputPath(job, new Path(analysisPath));
//		FileOutputFormat.setOutputPath(
//				job,
//				new Path(String.format("%s/%s/%s", outputPath,
//						TYPE_MOBILE_SDK_ACTIVE_USER,
//						Utility.timeToDate(timeStart))));
//
//		job.setMapperClass(MobileSDKActiveUsersMapper.class);
//		job.setCombinerClass(MobileSDKActiveUsersReducer.class);
//		job.setReducerClass(MobileSDKActiveUsersReducer.class);
//
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//		exitCode = job.waitForCompletion(true) ? 0 : 1;
//
//		long timeEnd = System.currentTimeMillis();
//		System.err.println("The total cost: " + (timeEnd - timeStart));
//
//		return exitCode;
//	}

//	private static int getMobileSDKAnalysisData(String inputPath,
//			String outputPath) throws IOException, InterruptedException,
//			ClassNotFoundException {
//		long timeStart = System.currentTimeMillis();
//
//		Job job = new Job();
//		job.setJarByClass(LogAnalyzerMR.class);
//		job.setJobName(JOB_MOBILE_SDK_ANALYSIS);
//		FileInputFormat.addInputPath(job, new Path(inputPath));
//		FileOutputFormat.setOutputPath(job, new Path(outputPath));
//
//		job.setMapperClass(MobileSDKAnalysisDataMapper.class);
//		job.setCombinerClass(MobileSDKAnalysisDataReducer.class);
//		job.setReducerClass(MobileSDKAnalysisDataReducer.class);
//
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//		int exitCode = job.waitForCompletion(true) ? 0 : 1;
//
//		long timeEnd = System.currentTimeMillis();
//		System.err.println("The total cost: " + (timeEnd - timeStart));
//
//		return exitCode;
//	}
	
	public static int getMobileSDKActiveUsers(String inputPath,
			String outputPath) throws IOException, InterruptedException,
			ClassNotFoundException {
		long timeStart = System.currentTimeMillis();
		
		Job job = new Job();
		job.setJarByClass(LogAnalyzerMR.class);
		job.setJobName(JOB_MOBILE_SDK_ACTIVE_USER);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setMapperClass(ActiveUserMapper.class);
		job.setReducerClass(ActiveUserReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		
		long timeEnd = System.currentTimeMillis();
		System.err.println("The total cost: " + (timeEnd - timeStart));
		
		return exitCode;
	}
	
	public static int getMobileSDKUserInfo(String inputPath,
			String outputPath) throws IOException, InterruptedException,
			ClassNotFoundException {
		long timeStart = System.currentTimeMillis();
		
		Job job = new Job();
		job.setJarByClass(LogAnalyzerMR.class);
		job.setJobName(JOB_MOBILE_SDK_USERINFO);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setMapperClass(MobileSDKUserInfoMapper.class);
		job.setReducerClass(MobileSDKUserInfoReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		
		long timeEnd = System.currentTimeMillis();
		System.err.println("The total cost: " + (timeEnd - timeStart));
		
		return exitCode;
	}
	
	public static int getMobileGameUserInfo(String inputPath,
			String outputPath) throws IOException, InterruptedException,
			ClassNotFoundException {
		long timeStart = System.currentTimeMillis();
		
		Job job = new Job();
		job.setJarByClass(LogAnalyzerMR.class);
		job.setJobName(JOB_MOBILE_GAME_USERINFO);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setMapperClass(GameUserInfoMapper.class);
		job.setReducerClass(GameUserInfoReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		
		long timeEnd = System.currentTimeMillis();
		System.err.println("The total cost: " + (timeEnd - timeStart));
		
		return exitCode;
	}

	public static int getMobileGameDownloadStatistic(String inputPath,
			String outputPath) throws IOException, InterruptedException,
			ClassNotFoundException {
		long timeStart = System.currentTimeMillis();

		Job job = new Job();
		job.setJarByClass(LogAnalyzerMR.class);
		job.setJobName(JOB_MOBILE_GAME_DOWNLOAD);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(
				job,
				new Path(String.format("%s/%s/%s", outputPath,
						TYPE_MOBILE_SDK_ACTIVE_USER,
						Utility.timeToDate(timeStart))));
		// FileOutputFormat.setCompressOutput(job, true);
		// FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

		 job.setMapperClass(MobileGameDownloadStatisticMapper.class);
//		job.setCombinerClass(MobileSDKActiveUsersReducer.class);
		// job.setNumReduceTasks(REDUCE_NUMBER);
		job.setReducerClass(MobileGameDownloadStatisticReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		int exitCode = job.waitForCompletion(true) ? 0 : 1;

		long timeEnd = System.currentTimeMillis();
		System.err.println("The total cost: " + (timeEnd - timeStart));

		return exitCode;
	}
}
