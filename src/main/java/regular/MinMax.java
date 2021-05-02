package regular;

import common.ReviewWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class MinMax {

    public static class MinMaxMapper extends Mapper<Object, Text, DoubleWritable, NullWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String rawString = itr.nextToken();
                ReviewWritable review = new ReviewWritable(rawString);
                if (!review.isVacantRating()) {
                    context.write(new DoubleWritable(review.getRating()), NullWritable.get());
                }
            }
        }
    }

    public static class MinMaxReducer extends Reducer<DoubleWritable, NullWritable, DoubleWritable, NullWritable> {
        private double minRating = Double.MAX_VALUE;
        private double maxRating = Double.MIN_VALUE;

        @Override
        protected void reduce(DoubleWritable key, Iterable<NullWritable> values, Context context) {
            minRating = Math.min(key.get(), minRating);
            maxRating = Math.max(key.get(), maxRating);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new DoubleWritable(minRating), NullWritable.get());
            context.write(new DoubleWritable(maxRating), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "min max");
        job.setJarByClass(Sampler.class);
        job.setMapperClass(MinMaxMapper.class);
        job.setReducerClass(MinMaxReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
