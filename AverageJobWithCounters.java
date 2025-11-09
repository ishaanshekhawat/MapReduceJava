package average;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageJob extends Configured implements Tool {

    // Counters to track valid and invalid input records
    public enum Counters {BAD_RECORDS, GOOD_RECORDS}

    /**
     * Mapper: 
     * Reads each line, validates the required integer field, and emits:
     * key = words[1], value = "<numeric-field>,1"
     * The ",1" tracks one occurrence for averaging.
     */
    public static class AverageMapper extends Mapper<LongWritable, Text, Text, Text> {
        public Text outputKey = new Text();
        public Text outputValue = new Text();
        public final String ONE = ",1";
        
        // Helper to throw if field can't be parsed as int
        public void checkInt(String str) throws NumberFormatException {
            Integer.parseInt(str);
        }
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Split on comma; expecting 12 columns
            String [] words = StringUtils.split(value.toString(), '\\', ',');

            // Process only rows with exactly 12 columns
            if (words.length == 12) {
                try {
                    // Validate the target numeric field at position 9
                    checkInt(words[9]);

                    // Emit key: column 1, value: "field9,1"
                    outputKey.set(words[1].trim());
                    outputValue.set(words[9] + ONE);
                    context.write(outputKey, outputValue);

                    context.getCounter(Counters.GOOD_RECORDS).increment(1);
                }
                catch (NumberFormatException e) {
                    // Field 9 not numeric
                    context.getCounter(Counters.BAD_RECORDS).increment(1);
                }
            }
            else {
                // Bad record structure
                context.getCounter(Counters.BAD_RECORDS).increment(1);
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            // Print counter summary at end of map task
            System.out.println("GOOD RECORDS counter = " + context.getCounter(Counters.GOOD_RECORDS).getValue());
            System.out.println("BAD RECORDS counter = " + context.getCounter(Counters.BAD_RECORDS).getValue());
        }
    }

    /**
     * Combiner:
     * Receives multiple mapper outputs like "value,count".
     * Locally aggregates them to reduce shuffle load.
     */
    public static class AverageCombiner extends Reducer<Text, Text, Text, Text> {
        private Text outputValue = new Text();
        private String COMMA = ",";
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long sum = 0;
            int count = 0;

            // Aggregate partial sums and counts
            for (Text val : values) {
                String[] words = StringUtils.split(val.toString(), '\\', ',');
                sum += Long.parseLong(words[0]);
                count += Integer.parseInt(words[1]);
            }

            // Emit combined value: "<sum>,<count>"
            outputValue.set(sum + COMMA + count);
            context.write(key, outputValue);
        }
    }

    /**
     * Reducer:
     * Receives fully combined values for each key, calculates final average.
     */
    public static class AverageReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        DoubleWritable outputValue = new DoubleWritable();
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long sum = 0;
            int count = 0;

            // Aggregate all values: "<sum>,<count>"
            for (Text val : values) {
                String[] words = StringUtils.split(val.toString(), '\\', ',');
                sum += Long.parseLong(words[0]);
                count += Integer.parseInt(words[1]);
            }

            // Final average
            outputValue.set(((double) sum) / count);
            context.write(key, outputValue);
        }
    }

    /**
     * Job configuration:
     * Sets mapper, reducer, combiner, input/output formats, and paths.
     */
    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, "AverageJob");
        job.setJarByClass(AverageJob.class);

        // Output path cleanup for repeated runs
        Path out = new Path("average");
        out.getFileSystem(conf).delete(out, true);

        FileInputFormat.setInputPaths(job, "counties");
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(AverageMapper.class);
        job.setReducerClass(AverageReducer.class);
        job.setCombinerClass(AverageCombiner.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int result = 0;
        try {
            result = ToolRunner.run(new Configuration(), new AverageJob(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(result);
    }
}
