import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Time;

public class Sampler {

//    private static File debugLogFile;
//    private static BufferedWriter debugOut;
    private static double sampleRate;

    public static class SampleMapper extends Mapper<Object, Text, CareerWritable, ReviewWritable> {

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

    public static class SampleReducer extends Reducer<CareerWritable, ReviewWritable, NullWritable, ReviewWritable> {
        private static final double sampleRate = 0.01;

        @Override
        protected void reduce(CareerWritable key, Iterable<ReviewWritable> reviews, Context context)
                throws IOException, InterruptedException {
            List<ReviewWritable> samples = new ArrayList<>();
            Random random = new Random(Time.getUtcTime());
            int cnt = 0;
            int layerSampleNum = (int) (1.0 * key.getCareerDataCount() * sampleRate);
            for (ReviewWritable review : reviews) {
                if (cnt < layerSampleNum) {
                    samples.add(review.clone());
                }
                else {
                    double randomDouble = random.nextDouble();
                    if (randomDouble < layerSampleNum * 1.0 / (cnt + 1)) {
                        int randomInt = random.nextInt(layerSampleNum);
                        samples.set(randomInt, review.clone());
                    }
                }
                cnt++;
            }
            for (ReviewWritable sample : samples) {
                context.write(NullWritable.get(), sample);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sample");
        job.setJarByClass(Sampler.class);
        job.setMapperClass(SampleMapper.class);
//        job.setCombinerClass(SampleReducer.class);
        job.setReducerClass(SampleReducer.class);
        job.setMapOutputKeyClass(CareerWritable.class);
        job.setMapOutputValueClass(ReviewWritable.class);
        job.setOutputKeyClass(CareerWritable.class);
        job.setOutputValueClass(ReviewWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        sampleRate = Double.parseDouble(args[2]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
