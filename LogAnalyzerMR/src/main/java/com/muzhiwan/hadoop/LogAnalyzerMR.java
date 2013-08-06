package com.muzhiwan.hadoop;

public class LogAnalyzerMR {

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err
					.println("Usage: LogAnalyzerMR <input type> <input path> <output path>");
			System.exit(-1);
		}
		
		int exitCode = JobRunner.executeAnalysis(args[0], args[1], args[2]);
		
		System.exit(exitCode);
	}
}
