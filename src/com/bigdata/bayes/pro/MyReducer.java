package com.bigdata.bayes.pro;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, IntWritable>{
	public void reduce(Text _key,Iterable<Text> values,Context context)
			throws IOException,InterruptedException{
		int sum=0;
		for (Text val:values){
			sum+=Integer.parseInt(val.toString());
		}
		context.write(_key, new IntWritable(sum));
	}
}
