package effective;

import common.CareerWritable;
import common.ReviewWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Time;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

public class SampleFilterMinMax {

    private static Path minMaxHPath;
    private static FileSystem minMaxFileSystem;
    private static double sampleRate;

    public static class SampleFilterMapper extends Mapper<Object, Text, CareerWritable, ReviewWritable> {

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

    public static class SampleFilterReducer extends Reducer<CareerWritable, ReviewWritable, NullWritable, ReviewWritable> {

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
        protected void reduce(CareerWritable key, Iterable<ReviewWritable> reviews, Context context)
                throws IOException, InterruptedException {
            List<ReviewWritable> samples = new ArrayList<>();
            Random random = new Random(Time.getUtcTime());
            int cnt = 0;
            int layerSampleNum = (int) (1.0 * key.getCareerDataCount() * sampleRate);
            for (ReviewWritable review : reviews) {
                if (cnt < layerSampleNum) {
                    samples.add(review.clone());
                } else {
                    double randomDouble = random.nextDouble();
                    if (randomDouble < layerSampleNum * 1.0 / (cnt + 1)) {
                        int randomInt = random.nextInt(layerSampleNum);
                        samples.set(randomInt, review.clone());
                    }
                }
                cnt++;
            }
            for (ReviewWritable sample : samples) {
                if (isLegalLatitude(sample.getLatitude()) && isLegalLongitude((sample.getLongitude()))) {
                    if (!sample.isVacantRating()) {
                        minRating = Math.min(minRating, sample.getRating());
                        maxRating = Math.max(maxRating, sample.getRating());
                        minUserIncome = Math.min(minUserIncome, sample.getUserIncome());
                        maxUserIncome = Math.max(maxUserIncome, sample.getUserIncome());
                        minAltitude = Math.min(minAltitude, sample.getAltitude());
                        maxAltitude = Math.max(maxAltitude, sample.getAltitude());
                        minLatitude = Math.min(minLatitude, sample.getLatitude());
                        maxLatitude = Math.max(maxLatitude, sample.getLatitude());
                        minLongitude = Math.min(minLongitude, sample.getLongitude());
                        maxLongitude = Math.max(maxLongitude, sample.getLongitude());
                    }
                    context.write(NullWritable.get(), sample);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException {
            OutputStream out = minMaxFileSystem.create(minMaxHPath);
            OutputStreamWriter outputStreamReader = new OutputStreamWriter(out, StandardCharsets.UTF_8);
            BufferedWriter minMaxWriter = new BufferedWriter(outputStreamReader);
            minMaxWriter.write(minRating + "\n" + maxRating + "\n");
            minMaxWriter.write(minUserIncome + "\n" + maxUserIncome + "\n");
            minMaxWriter.write(minAltitude + "\n" + maxAltitude + "\n");
            minMaxWriter.write(minLatitude + "\n" + maxLatitude + "\n");
            minMaxWriter.write(minLongitude + "\n" + maxLongitude + "\n");
            minMaxWriter.flush();
            minMaxWriter.close();
        }

        private static boolean isLegalLongitude(double longitude) {
            return longitude >= 8.1461259 && longitude <= 11.1993265;
        }

        private static boolean isLegalLatitude(double latitude) {
            return latitude >= 56.5824856 && latitude <= 57.750511;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sample");
        job.setJarByClass(SampleFilterMinMax.class);
        job.setMapperClass(SampleFilterMapper.class);
        job.setReducerClass(SampleFilterReducer.class);
        job.setMapOutputKeyClass(CareerWritable.class);
        job.setMapOutputValueClass(ReviewWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(ReviewWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        sampleRate = Double.parseDouble(args[2]);
        minMaxHPath = new Path(args[3]);
        minMaxFileSystem = minMaxHPath.getFileSystem(conf);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
