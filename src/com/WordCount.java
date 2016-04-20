package com;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
	/*
	 这个类实现Mapper借口中的map方法
	 输入参数中的value是文本文件中的一行
	 利用StringTokenizer将这个字符串拆成单词，
	 然后将输出结果<单词，1>
	 写入到org.apache.hadoop.mapred.OutputCollector中 
	*/
	public static class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private final IntWritable one = new IntWritable(1);
		private Text word = new Text();
		/*
		 代码中的LongWritable、IntWritable、Text都是Hadoop中实现的用于封装java数据类型的类，
		 这些类都能够被串行化从而便于在分布式环境中进行数据交换，
		 可以江他们分别视为long、in、String的替代品
		*/

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer token = new StringTokenizer(line);
			while (token.hasMoreTokens()) {
				word.set(token.nextToken());
				context.write(word, one); //output.collect(word, one);   OutputCollector<Text, IntWritable output, Reporter reporter
			}
		}
	}

	/*
	 这个类实现Reducer接口中的reduce方法，输入参数中的key、values是由Map任务输出的中间结果，values是一个Iterator
	*/
	public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			//遍历这个Iterator，就可以得到属于同一个key的所有values，此处，key是一个单词，values是词频
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		/*
		 在hadoop中一次计算任务称为一个job，可以通过一个JobConf对象设置如何运行这个job
		 此处定义了输出的key的类型是Text，value的类型是IntWritable
		*/
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(WordCount.class);
		job.setJobName("wordcount");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(WordCountMap.class);
		job.setReducerClass(WordCountReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		
		/*
		 ToolRunner的run方法开始，run方法需要3个参数，第一个是一个Configuration类的实例，第二个是WordCount类的实例，args就是从控制台接收到的命令行数组
		*/
		
	}
}
