package wordcount;

import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * A Hadoop MapReduce WordCount job with a custom Partitioner and logging.
 */
public class WordCountJob extends Configured implements Tool {
	
	/**
	 * Custom partitioner that assigns keys to reducers based on
	 * (word length * value) modulo number of reducers.
	 */
	public static class WordCountPartitioner extends Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {

			System.out.println("Inside Custom Partitioner Class");

			// If only one reducer is used, always send everything to reducer 0.
			if (numReduceTasks == 1) {
				return 0;
			}

			// Custom partition logic based on word length and count value.
			return (key.toString().length() * value.get()) % numReduceTasks;
		}
	}
	
	/**
	 * Mapper that emits (word, 1) pairs and logs setup, processing, and cleanup stages.
	 */
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private Logger logger = Logger.getLogger(WordCountMapper.class);

		@Override
		protected void setup(Context context) {
			// This runs once per mapper task before map() starts.
			logger.debug("Inside Setup");
		}
		
		private static final IntWritable ONE = new IntWritable(1);
		private Text outputKey = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			logger.debug("Inside map");

			// Convert Hadoop Text line into a Java String.
			String currentLine = value.toString();

			// Split line into words using space as delimiter.
			String[] words = StringUtils.split(currentLine, ' ');

			// Emit each word with count = 1.
			for (String word : words) {
				outputKey.set(word);
				context.write(outputKey, ONE);
			}
		}
		
		@Override
		protected void cleanup(Context context) {
			// Runs once after all map() calls are done.
			logger.debug("Inside CleanUp");
		}
	}
	
	/**
	 * Reducer that sums all integer counts for each word.
	 */
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable outputValue = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;

			// Sum all counts sent by mapper.
			for (IntWritable count : values) {
				sum += count.get();
			}

			outputValue.set(sum);

			// Emit (word, totalCount)
			context.write(key, outputValue);
		}
	}

	/**
	 * Configures and runs the MapReduce job.
	 */
	@Override
	public int run(String[] args) throws Exception {

		// Create a Job instance with the provided configuration.
		Job job = Job.getInstance(getConf(), "WordCountJob");
		Configuration conf = job.getConfiguration();
		job.setJarByClass(getClass());
		
		// Input and output file paths.
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);

		// Delete output path if it already exists (prevents "File exists" errors).
		out.getFileSystem(conf).delete(out, true);

		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		// Set Mapper, Reducer, and custom Partitioner.
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setPartitionerClass(WordCountPartitioner.class);

		// Set number of reducers manually if needed.
		// job.setNumReduceTasks(3);
		
		// Input/output format types.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Map and Reduce output types.
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Run job and return exit code.
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Main entry point for running this job using ToolRunner.
	 */
	public static void main(String[] args) {

		int result = 0;

		try {
			result = ToolRunner.run(new Configuration(), new WordCountJob(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.exit(result);
	}

}
