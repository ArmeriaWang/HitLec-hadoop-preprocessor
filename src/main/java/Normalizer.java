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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.StringTokenizer;

public class Normalizer {

    private static Path minMaxHPath;
    private static FileSystem minMaxFileSystem;

    public static class NormalizeMapper extends Mapper<Object, Text, CareerWritable, ReviewWritable> {
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

    public static class NormalizeReducer extends Reducer<CareerWritable, ReviewWritable, NullWritable, ReviewWritable> {

        private static double minRating;
        private static double maxRating;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            InputStream in = minMaxFileSystem.open(minMaxHPath);
            InputStreamReader inputStreamReader = new InputStreamReader(in, StandardCharsets.UTF_8);
            BufferedReader minMaxReader = new BufferedReader(inputStreamReader);
            String line = minMaxReader.readLine();
            minRating = Double.parseDouble(line);
            line = minMaxReader.readLine();
            maxRating = Double.parseDouble(line);
            super.setup(context);
        }

        @Override
        protected void reduce(CareerWritable key, Iterable<ReviewWritable> reviews, Context context)
                throws IOException, InterruptedException {
            for (ReviewWritable review : reviews) {
                ReviewWritable reviewOut = review.clone();
                if (!reviewOut.isVacantRating()) {
                    reviewOut.setRating(normalizeRating(review.getRating()));
                }
                reviewOut.setReviewDate(normalizeDate(review.getReviewDate()));
                reviewOut.setUserBirthday(normalizeDate(review.getUserBirthday()));
                reviewOut.setTemperature(normalizeTemperature(review.getTemperature()));
                context.write(NullWritable.get(), reviewOut);
            }
        }

        private static double normalizeRating(double rating) {
            if (maxRating - minRating < 1e-9) {
                return 0;
            }
            return (rating - minRating) / (maxRating - minRating);
        }

        private static String normalizeDate(String dateString) {
            if (dateString.contains("/")) {
                return dateString.replaceAll("/", "-");
            } else if (dateString.contains("-")) {
                return dateString;
            } else {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMMM d,yyyy", Locale.ENGLISH);
                LocalDate date = LocalDate.parse(dateString, formatter);
                return date.toString();
            }
        }

        private static String normalizeTemperature(String temperature) {
            if (temperature.endsWith("℉")) {
                double temp = Double.parseDouble(temperature.substring(0, temperature.length() - 1));
                return String.format("%.1f℃", (temp - 32) / 1.8);
            } else {
                return temperature;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "normalize");
        job.setJarByClass(Normalizer.class);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);
        job.setMapOutputKeyClass(CareerWritable.class);
        job.setMapOutputValueClass(ReviewWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(ReviewWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        minMaxHPath = new Path(args[2]);
        minMaxFileSystem = minMaxHPath.getFileSystem(conf);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
