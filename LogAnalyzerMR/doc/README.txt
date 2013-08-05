针对最新数据格式的分析代码。
MobileGameDownload 是分析下载量以及排行的代码
MobileSDKActiveUser 是分析手机 SDK 活跃用户的代码

这两部分功能都不会很完善，需要进一步实现。

hadoop jar LogAnalyzerMR.jar com.muzhiwan.hadoop.LogAnalyzerMR MobileGameDownload /apilogs/mobile/xxx.tar.gz /output/mobile
hadoop jar LogAnalyzerMR.jar com.muzhiwan.hadoop.LogAnalyzerMR MobileSDKActiveUser /apilogs/mobile/xxx.tar.gz /output/mobile
