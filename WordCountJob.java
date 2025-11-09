package wordcount;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
 * A simple Hadoop MapReduce job that counts the occurrences of each word in the input text.
 */
public class WordCountJob extends Configured implements Tool {
	
	/**
	 * Mapper class that takes each line of text and emits (word, 1) for each word.
	 */
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		// Constant writable to represent the count "1"
		private static final IntWritable ONE = new IntWritable(1);

		// Reusable Text object to avoid creating new objects during mapping
		private Text outputKey = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// Convert the line from Hadoop Text to Java String
			String currentLine = value.toString();

			// Split the line into words using Apache StringUtils
			String[] words = StringUtils.split(currentLine, ' ');

			// Emit each word with a count of 1
			for (String word : words) {
				outputKey.set(word);
				context.write(outputKey, ONE);
			}
		}
	}
	
	/**
	 * Reducer class that sums up all counts for each word.
	 */
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		// Reusable writable for final output (sum)
		private IntWritable outputValue = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			// Sum all counts for the given word
			int sum = 0;
			for (IntWritable count : values) {
				sum += count.get();
			}

			// Emit (word, totalCount)
			outputValue.set(sum);
			context.write(key, outputValue);
		}
	}

	/**
	 * Configures and runs the Hadoop job.
	 */
	@Override
	public int run(String[] args) throws Exception {

		// Create and configure a new MapReduce job
		Job job = Job.getInstance(getConf(), "WordCountJob");
		Configuration conf = job.getConfiguration();
		job.setJarByClass(getClass());
		
		// Set input and output paths
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);

		// Delete output folder if it already exists (prevents job failure)
		out.getFileSystem(conf).delete(out, true);
		
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		// Set mapper and reducer classes
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		// Set input and output format classes
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Set output key/value types for both map and reduce stages
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Run the job and return exit code
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Main method to run the WordCount job through ToolRunner.
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
