package com.xuwuji.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.xuwuji.hadoop.util.TimeUtil;

public class DistinctMapReduce {

	public static class DistinctMapReduceMap extends Mapper<Object, Text, Text, NullWritable> {

		private Text t = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split(" ");
			for (String s : strs) {
				t.set(s); // 这里用一个全局变量，每次去set值进去，每次都new Text这样的话会浪费，类似与new
							// String
				context.write(t, NullWritable.get());
			}
		}

	}

	/**
	 * The idea here is to use the default reducer behavior where the same keys
	 * are sent to one reducer.
	 * 
	 * When mapper emits keys and values, the output is shuffled across the
	 * nodes in the cluster. Here, the partitioner decides which keys should be
	 * reduced and on which node. On all the nodes, the same partitioning logic
	 * is used, which makes sure that the same keys are grouped together.
	 * 
	 * @author wuxu
	 *
	 */
	public static class DistinctMapReduceReduce extends Reducer<Text, NullWritable, Text, NullWritable> {

		public void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		String inputPath = WordCount.class.getResource("/input/json").getFile().toString();
		String outputPath = "/Users/wuxu/Project/git/hadoop-map-reduce/hadoop-mapreduce/output/"
				+ TimeUtil.currentTimewithMinutes();
		System.out.println(outputPath);
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "distinct");
		job.setJarByClass(DistinctMapReduce.class);

		job.setMapperClass(DistinctMapReduceMap.class);
		job.setCombinerClass(DistinctMapReduceReduce.class);
		job.setReducerClass(DistinctMapReduceReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
