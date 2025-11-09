package grep;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Grep extends Configured implements Tool {
    
    // Mapper class that searches for words containing a given substring
    public static class GrepMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private String searchString;               // substring to search for
        private Text outputValue = new Text();     // output key
        private final static IntWritable ONE = new IntWritable(1); // constant output value

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            System.out.println("-----Inside map()-----");

            // Split each input line into words. Delimiters: '\' and space.
            String[] words = StringUtils.split(value.toString(), '\\', ' ');

            for (String word : words) {

                // Check if the current word contains the search substring
                if (word.contains(searchString)) {
                    outputValue.set(word);
                    context.write(outputValue, ONE); // Emit (word, 1)
                }
            }
        }

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            System.out.println("-----Inside setup()-----");

            // Retrieve the substring to search for from job configuration.
            // This method runs once per Mapper task (not once per record).
            searchString = context.getConfiguration().get("searchString");
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        // Create and configure a new MapReduce job instance
        Job job = Job.getInstance(getConf(), "GrepJob");
        Configuration conf = job.getConfiguration();

        // Pass the search substring into the configuration for mappers to read
        conf.set("searchString", args[2]);

        job.setJarByClass(getClass());

        // Input and output paths
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);

        // Clean output directory if it already exists
        out.getFileSystem(conf).delete(out, true);

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        // Set the Mapper class
        job.setMapperClass(GrepMapper.class);

        // Reducer that sums values for identical keys
        // IntSumReducer emits (Text, IntWritable)
        job.setReducerClass(IntSumReducer.class);

        // Input and output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Mapper output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Run job and return 0 if success else 1
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int result = 0;
        try {
            // Launch the Hadoop job using the ToolRunner helper
            result = ToolRunner.run(new Configuration(),
                    new Grep(),
                    args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(result);
    }
}
