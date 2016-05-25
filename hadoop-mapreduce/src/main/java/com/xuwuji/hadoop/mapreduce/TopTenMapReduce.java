package com.xuwuji.hadoop.mapreduce;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.xuwuji.hadoop.mapreduce.WordCount.IntSumReducer;
import com.xuwuji.hadoop.mapreduce.WordCount.TokenizerMapper;
import com.xuwuji.hadoop.util.TimeUtil;

/**
 * find top 2 record from some records
 * 
 * @author wuxu
 *
 */
public class TopTenMapReduce {

	public static class TopTenMapper extends Mapper<Object, Text, Text, IntWritable> {

		// Tree map keeps records sorted by key
		private TreeMap<Integer, String> countWordMap = new TreeMap<Integer, String>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] words = value.toString().split(" ");
			for (String s : words) {
				System.out.println(s);
			}

			String word = words[0];
			int count = Integer.parseInt(words[1]);

			countWordMap.put(count, word);

			if (countWordMap.size() > 2) {
				countWordMap.remove(countWordMap.firstKey());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Entry<Integer, String> entry : countWordMap.entrySet()) {
				context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
			}
		}
	}

	public static class TopTenReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		// Tree map keeps records sorted by key
		private TreeMap<IntWritable, Text> countWordMap = new TreeMap<IntWritable, Text>();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			for (IntWritable value : values) {
				countWordMap.put(value, key);
			}
			if (countWordMap.size() > 2) {
				countWordMap.remove(countWordMap.firstKey());
			}

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Entry<IntWritable, Text> entry : countWordMap.descendingMap().entrySet()) {
				context.write(entry.getValue(), entry.getKey());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		String inputPath = WordCount.class.getResource("/input/sample.txt").getFile().toString();
		String outputPath = "/Users/wuxu/Project/git/hadoop-map-reduce/hadoop-mapreduce/output/"
				+ TimeUtil.currentTimewithMinutes();
		System.out.println(outputPath);
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "top count");
		job.setJarByClass(TopTenMapReduce.class);

		job.setMapperClass(TopTenMapper.class);
		job.setCombinerClass(TopTenReducer.class);
		job.setReducerClass(TopTenReducer.class);

		job.setNumReduceTasks(1);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
