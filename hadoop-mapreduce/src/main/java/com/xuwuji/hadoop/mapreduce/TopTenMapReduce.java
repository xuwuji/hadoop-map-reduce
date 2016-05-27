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
			String[] words = value.toString().split("	");

			String word = words[0];
			int count = Integer.parseInt(words[1]);

			countWordMap.put(count, word);

			if (countWordMap.size() > 5) {
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
		private TreeMap<Integer, String> countWordMap = new TreeMap<Integer, String>();

		public void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println("[" + Thread.currentThread().getName() + "]  --- " + countWordMap.size());
			System.out.println("[" + Thread.currentThread().getName() + "]  --- " + key);
			for (IntWritable value : values) {
				System.out.println("[" + Thread.currentThread().getName() + "]  --- " + value);
				countWordMap.put(value.get(), key.toString());
				System.out.println("[" + Thread.currentThread().getName() + "]  --- put " + value + key);
			}
			if (countWordMap.size() > 2) {
				System.out
						.println("[" + Thread.currentThread().getName() + "]  --- removing " + countWordMap.firstKey());
				countWordMap.remove(countWordMap.firstKey());
			}
			System.out.println("[" + Thread.currentThread().getName() + "]  --- " + countWordMap.size());
		}

		/**
		 * 在最后才调用，由于只有一个reducer，所以所有的data都会经过这个reducer，而map中只保存两个data，
		 * 所以最后输出的是最大的两个data
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Entry<Integer, String> entry : countWordMap.descendingMap().entrySet()) {
				context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		String inputPath = WordCount.class.getResource("/input/top").getFile().toString();
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
