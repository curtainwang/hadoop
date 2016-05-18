package com.hadoop.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {

	
	public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text>{
		public static final String LEFT_FILENAME = "student_info.txt";
		public static final String RIGHT_FILENAME = "student_class_info.txt";
		public static final String LEFT_FILENAME_FLAG = "l";
		public static final String RIGHT_FILENAME_FLAG = "r";
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//get the road of HDFS
			String filePath = ((FileSplit) context.getInputSplit()).getPath().toString();
			String fileFlag = null;
			String joinKey = null;
			String joinValue = null;
			
			//judge where does the record come from
			if(filePath.contains(LEFT_FILENAME)){
				fileFlag = LEFT_FILENAME_FLAG;
				joinKey = value.toString().split("\t")[1];
				joinValue = value.toString().split("\t")[0];
			}else if(filePath.contains(RIGHT_FILENAME)) {
				fileFlag = RIGHT_FILENAME_FLAG;
				joinKey = value.toString().split("\t")[0];
				joinValue = value.toString().split("\t")[1];
			}
			
			//output the key and mark where does the result come from
			context.write(new Text(joinKey), new Text(joinValue + "\t" + fileFlag));
			
		}	
	}
	
	
	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		
		public static final String LEFT_FILENAME = "student_info.txt";
		public static final String RIGHT_FILENAME = "student_class_info.txt";
		public static final String LEFT_FILENAME_FLAG = "l";
		public static final String RIGHT_FILENAME_FLAG = "r";
		
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> iterator = values.iterator();
			List<String> studentClassNames = new ArrayList<String>();
			String studentName = "";
			while(iterator.hasNext()){
				String[] infos = iterator.next().toString().split("\t");
				//judge where does the record come from
				if(infos[1].equals(LEFT_FILENAME_FLAG)){
					studentName = infos[0];
				}else if(infos[1].equals(RIGHT_FILENAME_FLAG)){
					studentClassNames.add(infos[0]);
				}
			}
			
			//求笛卡尔积
			for(int i = 0; i < studentClassNames.size(); i++){
				context.write(new Text(studentName), new Text(studentClassNames.get(i)));
			}
			
		}
	}
	
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		// TODO Auto-generated method stub
		Configuration configuration = new Configuration();
		Job job = new Job(configuration, "join something");
		job.setJarByClass(Driver.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true)?0:1);
		

	}

}
