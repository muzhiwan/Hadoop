针对历史数据格式的分析代码。
具体的分析项不是太重要，以后可能都可以重构。
三个参数：分析项 输入目录 输出目录
输入目录的规则： apilogs 表明都是接口服务器导出来的日志，legacy 表示历史遗留数据，client 在这里表示手机客户端，2 表示是第 2 种日志格式（目前没有做清晰的映射表，但是已经整理出来了历史遗留数据的格式种类）

hadoop jar LegacyAndroidClientLogsMR.jar com.muzhiwan.hadoop.LegacyAndroidClientLogsMR PopularGame2 /apilogs/legacy/client/2 /output/legacy
hadoop jar LegacyAndroidClientLogsMR.jar com.muzhiwan.hadoop.LegacyAndroidClientLogsMR GameDownload2 /apilogs/legacy/client/2 /output/legacy
hadoop jar LegacyAndroidClientLogsMR.jar com.muzhiwan.hadoop.LegacyAndroidClientLogsMR DailyDownload2 /apilogs/legacy/client/2 /output/legacy
hadoop jar LegacyAndroidClientLogsMR.jar com.muzhiwan.hadoop.LegacyAndroidClientLogsMR ErrorCategory2 /apilogs/legacy/client/2 /output/legacy
