package com.xuwuji.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.xuwuji.hadoop.util.TimeUtil;

/**
 * We know that when we execute the map reduce code, a lot of data gets
 * transferred over the network when this shuffle takes place. Sometimes, the
 * size of the keys and values that are transferred can be huge, which might
 * affect network traffic. To avoid congestion, it’s very important to send the
 * data in a serialized format. To abstract the pain of serialization and
 * deserialization from the map reduce programmer, Hadoop has introduced a set
 * of box/wrapper classes such as IntWritable, LongWritable, Text,
 * FloatWritable, DoubleWritable, and so on. These are wrapper classes on top of
 * primitive data types, which can be serialized and deserialized easily. The
 * keys need to be WritableComparable, while the values need to be Writable.
 * Technically, both keys and values are WritableComparable.
 * 
 * @author wuxu
 *
 */
public class WordCount {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			System.out.println(key + " " + result);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		String inputPath = WordCount.class.getResource("/input/json/sample.txt").getFile().toString();

		String outputPath = "/Users/wuxu/Project/git/hadoop-map-reduce/hadoop-mapreduce/output/"
				+ TimeUtil.currentTimewithMinutes();
		System.out.println(outputPath);
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
