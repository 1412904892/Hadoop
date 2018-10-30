package com.bigdata.bayes.tosequencefile;

import java.io.IOException;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 
//将整个文件作为一条记录处理
public class WholeFileInputFormat extends FileInputFormat<NullWritable,Text>{
 
	//表示文件不可分
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
 
	@Override
	public RecordReader<NullWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		WholeRecordReader reader=new WholeRecordReader();
		reader.initialize(split, context);
		return reader;
	}
  
}
