package com.bigdata.bayes.pro;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<NullWritable, Text, Text, Text>{
	
	public void map(NullWritable ikey,Text ival,Context context)
			throws IOException,InterruptedException{
		String []line=ival.toString().split(":| ");
		int size=line.length-1;
		context.write(new Text(line[0]), new Text(String.valueOf(size)));
	}
}
