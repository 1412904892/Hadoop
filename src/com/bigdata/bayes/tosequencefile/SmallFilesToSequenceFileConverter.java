package com.bigdata.bayes.tosequencefile;
 
import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class SmallFilesToSequenceFileConverter{
	private static class SequenceFileMapper extends Mapper<NullWritable,Text,Text,Text>
	{
		private Text filenameKey;
		//setup在task之前调用，用来初始化filenamekey
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			InputSplit split=context.getInputSplit();
		    Path path=((FileSplit)split).getPath();
		    filenameKey=new Text(path.toString());
		}
		
		@Override
		protected void map(NullWritable key,Text value,Context context)
				throws IOException, InterruptedException {
			context.write(filenameKey, value);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"SmallFilesToSequenceFileConverter");
		
		job.setJarByClass(SmallFilesToSequenceFileConverter.class);
		
		job.setInputFormatClass(WholeFileInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		
		job.setMapperClass(SequenceFileMapper.class);
		
		//设置最终的输出
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
	
	    System.exit(job.waitForCompletion(true) ? 0:1);
	}
}
 
