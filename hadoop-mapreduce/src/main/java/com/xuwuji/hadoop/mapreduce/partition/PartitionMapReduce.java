package com.xuwuji.hadoop.mapreduce.partition;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class PartitionMapReduce {

	/**
	 * 
	 * @author wuxu
	 * 
	 *         第一个参数为mapper输出后的key，根据这个key进行partition，
	 *         第二个参数是mapper这个key处理输出的value
	 *
	 */
	public class MyPartitioner extends Partitioner<IntWritable, Text> implements Configurable {

		private Configuration conf = null;

		@Override
		public int getPartition(IntWritable key, Text value, int numPartitions) {
			return key.get() % 10;
		}

		public Configuration getConf() {
			return conf;
		}

		public void setConf(Configuration conf) {
			this.conf = conf;
		}
	}

	public static class MyMapper extends Mapper<Object, Text, IntWritable, Text> {
		public void map(Object key, Text value, Mapper<Object, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String words[] = value.toString().split("[|]");
			context.write(new IntWritable(Integer.parseInt(words[2])), value);
		}
	}

	public static class MyReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
		protected void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text t : values) {
				context.write(t, NullWritable.get());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "partiotion");
		job.setJarByClass(PartitionMapReduce.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(MyPartitioner.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
