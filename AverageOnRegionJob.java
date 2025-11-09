package average;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageJob extends Configured implements Tool {

    // Mapper that appends a region label to each line
    public static class AverageMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        public Text outputValue = new Text();

        /**
         * Adds a region label based on the first character of the county name.
         * Counties starting with A-M are tagged as "Southern", others as "Northern".
         */
        protected String setRegion(String key) {
            if (key.charAt(0) >= 'A' && key.charAt(0) <= 'M') {
                return ", Southern";
            } else {
                return ", Northern";
            }
        }

        /**
         * Reads each input line, splits it, determines the region,
         * appends the region string, and writes the updated line.
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String currentLine = value.toString();

            // Split line by comma. Extracts fields such as county name (words[1]).
            String[] words = StringUtils.split(currentLine, '\\', ',');

            // Append region tag to the original line.
            currentLine = currentLine.concat(setRegion(words[1].trim()));

            outputValue.set(currentLine);
            context.write(NullWritable.get(), outputValue);
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, "AverageJob");
        job.setJarByClass(AverageJob.class);

        // Output path cleanup to avoid job failure due to existing directory.
        Path out = new Path("averagenew");
        out.getFileSystem(conf).delete(out, true);

        // Set input and output directories.
        FileInputFormat.setInputPaths(job, "counties");
        FileOutputFormat.setOutputPath(job, out);

        // Set mapper class only (no reducer --> map-only job).
        job.setMapperClass(AverageMapper.class);

        // Set input & output format.
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set output types.
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // Execute job and return status code.
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int result = 0;
        try {
            // Run job using Hadoop ToolRunner.
            result = ToolRunner.run(new Configuration(), new AverageJob(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.exit(result);
    }
}
