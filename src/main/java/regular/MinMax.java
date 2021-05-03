package regular;

import common.CareerWritable;
import common.ReviewWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

public class MinMax {

    private static final Random random = new Random();

    public static class MinMaxMapper extends Mapper<Object, Text, IntWritable, ReviewWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String rawString = itr.nextToken();
                ReviewWritable review = new ReviewWritable(rawString);
                if (!review.isVacantRating()) {
                    context.write(new IntWritable(random.nextInt()), review);
                }
            }
        }
    }

    public static class MinMaxReducer extends Reducer<IntWritable, ReviewWritable, DoubleWritable, NullWritable> {
        private double minRating = Double.MAX_VALUE;
        private double maxRating = Double.MIN_VALUE;
        private double minUserIncome = Double.MAX_VALUE;
        private double maxUserIncome = Double.MIN_VALUE;
        private double minLatitude = Double.MAX_VALUE;
        private double maxLatitude = Double.MIN_VALUE;
        private double minLongitude = Double.MAX_VALUE;
        private double maxLongitude = Double.MIN_VALUE;
        private double minAltitude = Double.MAX_VALUE;
        private double maxAltitude = Double.MIN_VALUE;

        @Override
        protected void reduce(IntWritable key, Iterable<ReviewWritable> values, Context context) {
            for (ReviewWritable review : values) {
                if (!review.isVacantRating()) {
                    minRating = Math.min(minRating, review.getRating());
                    maxRating = Math.max(maxRating, review.getRating());
                }
                if (!review.isVacantUserIncome()) {
                    minUserIncome = Math.min(minUserIncome, review.getUserIncome());
                    maxUserIncome = Math.max(maxUserIncome, review.getUserIncome());
                }
                minAltitude = Math.min(minAltitude, review.getAltitude());
                maxAltitude = Math.max(maxAltitude, review.getAltitude());
                minLatitude = Math.min(minLatitude, review.getLatitude());
                maxLatitude = Math.max(maxLatitude, review.getLatitude());
                minLongitude = Math.min(minLongitude, review.getLongitude());
                maxLongitude = Math.max(maxLongitude, review.getLongitude());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new DoubleWritable(minRating), NullWritable.get());
            context.write(new DoubleWritable(maxRating), NullWritable.get());
            context.write(new DoubleWritable(minUserIncome), NullWritable.get());
            context.write(new DoubleWritable(maxUserIncome), NullWritable.get());
            context.write(new DoubleWritable(minAltitude), NullWritable.get());
            context.write(new DoubleWritable(maxAltitude), NullWritable.get());
            context.write(new DoubleWritable(minLatitude), NullWritable.get());
            context.write(new DoubleWritable(maxLatitude), NullWritable.get());
            context.write(new DoubleWritable(minLongitude), NullWritable.get());
            context.write(new DoubleWritable(maxLongitude), NullWritable.get());
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
