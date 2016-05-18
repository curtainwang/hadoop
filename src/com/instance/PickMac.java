package com.instance;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
public class PickMac extends Configuration implements Tool {

	enum Counter{
		LINESKIP; //出错的行
	}
	
	public static class Map extends Mapper<LongWritable, Text, NullWritable, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			try{
				//deal with the data
				String [] lineSplit = line.split("");
				String month = lineSplit[0];
				String time = lineSplit[1];
				String mac = lineSplit[6];
				Text out = new Text(month + ' ' + time + ' ' + mac);
				
				context.write(NullWritable.get(), out);
				
			}catch(java.lang.ArrayIndexOutOfBoundsException e){
				context.getCounter(Counter.LINESKIP).increment(1);
				return ;
			}
		}
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = getConf();
		Job job = new Job(conf, "PickMac");
		job.setJarByClass(PickMac.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(Map.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);
		
		return job.isSuccessful() ? 0:1;
		
		
		return 0;
	}
	
	*/

	/**
	 * @param args
	 */
/*	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), (Tool) new PickMac(), args);
		System.exit(res);

	}


	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub
		
	}

}
*/