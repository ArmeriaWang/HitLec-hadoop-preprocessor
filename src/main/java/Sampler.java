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
import org.apache.hadoop.util.Time;

public class Sampler {

//    private static File debugLogFile;
//    private static BufferedWriter debugOut;

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
        private static final double sampleRate = 0.01;

        public void reduce(CareerWritable key, Iterable<ReviewWritable> reviews, Context context)
                throws IOException, InterruptedException {
            List<ReviewWritable> samples = new ArrayList<>();
            Random random = new Random(Time.getUtcTime());
            File debugLogFile = new File(String.format("/home/armeria/debug_info_%s.txt", key.getCareer().ordinal()));
            if (!debugLogFile.exists()) {
                debugLogFile.createNewFile();
            }
            BufferedWriter debugOut = new BufferedWriter(new FileWriter(debugLogFile));
            int cnt = 0;
            int layerSampleNum = (int) (1.0 * key.getCareerDataCount() * sampleRate);
            for (ReviewWritable review : reviews) {
                debugOut.write(String.format("\n\ncnt = %d  layerSampleNum = %d\n", cnt, layerSampleNum) + review.toString() + "\n");
                debugOut.flush();
                if (cnt < layerSampleNum) {
                    samples.add(review);
                } else {
                    double randomDouble = random.nextDouble();
                    debugOut.write(String.format("rDouble = %.2f\n", randomDouble));
                    if (randomDouble < layerSampleNum * 1.0 / (cnt + 1)) {
                        int randomInt = random.nextInt(layerSampleNum);
                        debugOut.write(String.format("This replace %d-th, rId=%s\n", randomInt, samples.get(randomInt).getReviewId()));
                        samples.set(randomInt, review);
                    }
                }
                cnt++;
            }
            debugOut.close();
            debugLogFile = new File(String.format("/home/armeria/real_samples_%s.txt", key.getCareer().ordinal()));
            if (!debugLogFile.exists()) {
                debugLogFile.createNewFile();
            }
            debugOut = new BufferedWriter(new FileWriter(debugLogFile));
            for (int i = 0; i < samples.size(); i++) {
                ReviewWritable sample = samples.get(i);
                debugOut.write(sample.toString() + "\n");
                debugOut.flush();
                context.write(new IntWritable(key.getCareer().ordinal()), new Text(sample.getReviewId()));
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
