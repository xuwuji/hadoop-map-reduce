package com.xuwuji.hadoop.mapreduce.json;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xuwuji.hadoop.counter.RecordCounter;
import com.xuwuji.hadoop.mapreduce.WordCount;
import com.xuwuji.hadoop.util.TimeUtil;

import java.io.IOException;

public final class JsonMapReduce {
	private static final Logger log = LoggerFactory.getLogger(JsonMapReduce.class);

	/**
	 * 
	 * @author wuxu 第一个参数是文件中输入的类型，在这里txt文件里每个都是LongWritable
	 * 
	 *         第两个参数在map接收到需要处理的类型，
	 *         在job.setInputFormatClass(JsonInputFormat.class)
	 *         定义了需要JsonInputFormat这个class对txt里面的每个LongWritable类型的数据进行处理后，
	 *         转成MapWritable的类型给map进行处理
	 * 
	 *         后两个参数是经过map处理后输出的k和v
	 */
	public static class KeyWordMapper extends Mapper<LongWritable, MapWritable, Text, IntWritable> {
		final IntWritable one = new IntWritable(1);
		private final Text happy = new Text("happy");
		private final Text sad = new Text("sad");

		@Override
		protected void map(LongWritable key, MapWritable value, Context context)
				throws IOException, InterruptedException {
			final IntWritable one = new IntWritable(1);
			Text mapValue = (Text) value.get(new Text("keyword"));
			System.out.println(mapValue);
			// increment the count for each record
			context.getCounter(RecordCounter.JSON_RECORD_COUNT).increment(1L);
			if (mapValue.toString().equals("happy")) {
				context.write(happy, one);
			} else {
				context.write(sad, one);
			}
		}
	}

	/**
	 * 前两个参数是从map接收到的k和v的类型
	 * 
	 * 后两个参数为输出的k和v类型
	 * 
	 * @author wuxu
	 *
	 */
	public static class CountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			IntWritable result = new IntWritable();
			
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}

	}

	/**
	 * Main entry point for the example.
	 *
	 * @param args
	 *            arguments
	 * @throws Exception
	 *             when something goes wrong
	 */
	public static void main(final String[] args) throws Exception {
		String inputPath = WordCount.class.getResource("/input/json").getFile().toString();
		String outputPath = "/Users/wuxu/Project/git/hadoop-map-reduce/hadoop-mapreduce/output/"
				+ TimeUtil.currentTimewithMinutes();

		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(JsonMapReduce.class);

		job.setMapperClass(KeyWordMapper.class);
		job.setCombinerClass(CountReduce.class);
		job.setReducerClass(CountReduce.class);

		job.setInputFormatClass(JsonInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		Counters counters = job.getCounters();
		System.out.println("Total Records :" + counters.findCounter(RecordCounter.JSON_RECORD_COUNT).getValue());

	}

}
