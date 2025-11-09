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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * WordCount with a Combiner to reduce network shuffle.
 * Combiner performs local aggregation: (word, 1,1,1,...) -> (word, partialSum)
 *
 * Note:
 * - Combiners must be associative and commutative. This reducer is safe to reuse as a combiner.
 */
public class WordCountJob extends Configured implements Tool {
	
	/**
	 * Mapper: emits (word, 1) for each token.
	 */
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private static final IntWritable ONE = new IntWritable(1);
		private final Text outputKey = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String currentLine = value.toString();

			// Naive whitespace split; consider better tokenization for production.
			String[] words = StringUtils.split(currentLine, ' ');
			for (String word : words) {
				if (word == null || word.isEmpty()) continue;
				outputKey.set(word);
				context.write(outputKey, ONE);
			}
		}
	}
	
	/**
	 * Combiner: identical logic to reducer (sums counts).
	 * Runs on mapper outputs before shuffle to reduce data volume.
	 * IMPORTANT: Hadoop may run a combiner 0 or more times; do not rely on it always running.
	 */
	public static class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		private final IntWritable outputValue = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : values) {
				sum += count.get();
			}
			outputValue.set(sum);
			context.write(key, outputValue);
		}
	}
	
	/**
	 * Reducer: final global aggregation (word -> totalCount).
	 */
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private final IntWritable outputValue = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : values) {
				sum += count.get();
			}
			outputValue.set(sum);
			context.write(key, outputValue);
		}
	}

	/**
	 * Job configuration and submission.
	 * args[0] = input path, args[1] = output path
	 */
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "WordCountJob");
		Configuration conf = job.getConfiguration();
		job.setJarByClass(getClass());
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);

		// WARNING: Force-deletes the output directory if it exists.
		out.getFileSystem(conf).delete(out, true);

		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		// Mapper / Reducer / Combiner
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setCombinerClass(WordCountCombiner.class);
		
		// I/O formats
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Map output types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Final output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

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
