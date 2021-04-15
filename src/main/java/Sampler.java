import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sampler {

    private static File debugLogFile;
    private static BufferedWriter debugOut;

    public static class ReviewMapper extends Mapper<Object, Text, CareerWritable, ReviewWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String rawString = itr.nextToken();
                ReviewWritable review = new ReviewWritable(rawString);
                context.write(CareerWritable.valueOf(review.getUserCareer()), review);
            }
        }
    }

    public static class SampleReducer extends Reducer<CareerWritable, ReviewWritable, IntWritable, Text> {
        private final List<ReviewWritable> samples = new ArrayList<>();
        private final Random random = new Random();
        private static final double sampleRate = 0.01;

        public void reduce(CareerWritable key, Iterable<ReviewWritable> reviews, Context context)
                throws IOException, InterruptedException {
            debugLogFile = new File(String.format("/home/armeria/debug_info_%s.txt", key.getCareer().ordinal()));
            if (!debugLogFile.exists()) {
                debugLogFile.createNewFile();
            }
            debugOut = new BufferedWriter(new FileWriter(debugLogFile));
            int cnt = 0;
            int layerSampleNum = (int) (1.0 * key.getCareerDataCount() * sampleRate);
            for (ReviewWritable review : reviews) {
                debugOut.write(String.format("\ncnt = %d  layerSampleNum = %d\n", cnt, layerSampleNum) + review.toString());
                if (cnt < layerSampleNum) {
                    samples.add(review);
                } else {
                    if (Math.random() < layerSampleNum * 1.0 / (cnt + 1)) {
                        samples.set(random.nextInt(layerSampleNum), review);
                    }
                }
                cnt++;
            }
            for (ReviewWritable review : samples) {
                context.write(new IntWritable(key.getCareer().ordinal()), new Text(review.getReviewId()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sample by layer (career)");
        job.setJarByClass(Sampler.class);
        job.setMapperClass(ReviewMapper.class);
//        job.setCombinerClass(SampleReducer.class);
        job.setReducerClass(SampleReducer.class);
        job.setMapOutputKeyClass(CareerWritable.class);
        job.setMapOutputValueClass(ReviewWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
