import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.StringTokenizer;

public class Filter {

    public static class FilterMapper extends Mapper<Object, Text, CareerWritable, ReviewWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String rawString = itr.nextToken();
                ReviewWritable review = new ReviewWritable(rawString);
                context.write(CareerWritable.valueOf(review.getUserCareer()), review);
            }
        }
    }

    public static class FilterReducer extends Reducer<CareerWritable, ReviewWritable, NullWritable, ReviewWritable> {
        @Override
        protected void reduce(CareerWritable key, Iterable<ReviewWritable> reviews, Context context)
                throws IOException, InterruptedException {
            for (ReviewWritable review : reviews) {
                if (isLegalLatitude(review.getLatitude()) && isLegalLongitude((review.getLongitude()))) {
                    context.write(NullWritable.get(), review);
                }
            }
        }
    }

    private static boolean isLegalLongitude(double longitude) {
        return longitude >= 8.1461259 && longitude <= 11.1993265;
    }

    private static boolean isLegalLatitude(double latitude) {
        return latitude >= 56.5824856 && latitude <= 57.750511;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "filter");
        job.setJarByClass(Filter.class);
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);
        job.setMapOutputKeyClass(CareerWritable.class);
        job.setMapOutputValueClass(ReviewWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(ReviewWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
