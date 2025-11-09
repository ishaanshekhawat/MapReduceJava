package compress;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CompressDemoJob extends Configured implements Tool {

    /**
     * Mapper class:
     * Reads each line, splits it by space, checks if any word
     * contains the provided search string.  
     * If yes, emits a key built from words[0] + words[2] and the full line as value.
     */
    public static class CompressMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String searchString;
        private Text outputKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Split the input line into words
            String[] words = value.toString().split(" ");

            // Scan each word for the search substring
            for (String word : words) {
                if (word.contains(searchString)) {

                    // Build the mapper output key using the 1st and 3rd tokens
                    // Assumes the line has at least 3 tokens
                    outputKey.set(words[0] + " " + words[2]);

                    // Emit full line as value
                    context.write(outputKey, value);
                }
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Fetch the search string passed through configuration
            searchString = context.getConfiguration().get("searchString");
        }
    }

    /**
     * Reducer class:
     * Gets all values for each key and simply outputs them.
     * The output key is NullWritable to avoid adding the key back in the final result.
     */
    public static class CompressReducer extends Reducer<Text, Text, NullWritable, Text> {

        private final NullWritable outputKey = NullWritable.get();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Emit each record as-is
            for (Text v : values) {
                context.write(outputKey, v);
            }
        }
    }

    /**
     * Job configuration and execution.
     */
    @Override
    public int run(String[] args) throws Exception {

        // Create job instance with configuration
        Job job = Job.getInstance(getConf(), "CompressJob");
        Configuration conf = job.getConfiguration();

        // Pass the search string from CLI args to the job
        conf.set("searchString", args[0]);

        job.setJarByClass(CompressDemoJob.class);

        // Input and output paths
        Path out = new Path("logresults1");
        out.getFileSystem(conf).delete(out, true); // Delete output dir if exists
        FileInputFormat.setInputPaths(job, new Path("logfiles"));
        FileOutputFormat.setOutputPath(job, out);

        // Mapper and reducer configuration
        job.setMapperClass(CompressMapper.class);
        job.setReducerClass(CompressReducer.class);

        // Input/output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Output types
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Enable compression for map output
        conf.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);
        conf.setClass(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, SnappyCodec.class, CompressionCodec.class);

        // Enable compression for final output
        conf.setBoolean(FileOutputFormat.COMPRESS, true);
        conf.setClass(FileOutputFormat.COMPRESS_CODEC, SnappyCodec.class, CompressionCodec.class);

        // Submit job and wait
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Entry point for launching via ToolRunner.
     */
    public static void main(String[] args) {
        int result = 0;
        try {
            result = ToolRunner.run(new Configuration(), new CompressDemoJob(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(result);
    }
}
