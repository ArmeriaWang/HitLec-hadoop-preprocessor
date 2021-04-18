import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;

public class Filler {
    public static class FillerMapper extends Mapper<Object, Text, IntWritable, ReviewWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Random random = new Random();
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

    public static class FillerReducer extends Reducer<IntWritable, ReviewWritable, NullWritable, ReviewWritable> {
        private final int len = 5;
        private final double[] w = new double[len];
        private final double learningRate = 0.01;
        private final Set<ReviewWritable> vacantRatingReviews = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            for (int i = 0; i < len; i++) {
                w[i] = 0;
            }
            vacantRatingReviews.clear();
        }

        @Override
        protected void reduce(IntWritable key, Iterable<ReviewWritable> reviews, Context context)
                throws IOException, InterruptedException {
            double[] wPre = new double[len];
            for (ReviewWritable review : reviews) {
                System.arraycopy(w, 0, wPre, 0, 4);
                if (review.isVacantUserIncome()) {
                    continue;
                } else if (review.isVacantRating()) {
                    vacantRatingReviews.add(review.clone());
                    continue;
                }
                context.write(NullWritable.get(), review);
                double[] x = getParameters(review);
                double delta = review.getRating() - getProduct(x, wPre);
                for (int i = 0; i < len; i++) {
                    w[i] = w[i] + learningRate * delta * wPre[i];
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (ReviewWritable review : vacantRatingReviews) {
                double[] x = getParameters(review);
                review.setRating(getProduct(x, w));
                context.write(NullWritable.get(), review);
            }
            super.cleanup(context);
        }

        private double getProduct(double[] v1, double[] v2) {
            double ret = 0;
            for (int i = 0; i < len; i++) {
                ret += v1[i] * v2[i];
            }
            return ret;
        }

        private double[] getParameters(ReviewWritable review) {
            double[] ret = new double[len];
            ret[0] = review.getUserIncome();
            ret[1] = review.getLatitude();
            ret[2] = review.getLongitude();
            ret[3] = review.getAltitude();
            ret[4] = 1;
            return ret;
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "min max");
        job.setJarByClass(Sampler.class);
        job.setMapperClass(FillerMapper.class);
        job.setReducerClass(FillerReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(ReviewWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(ReviewWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
