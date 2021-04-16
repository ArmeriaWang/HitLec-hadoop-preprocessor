import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.nio.charset.StandardCharsets;
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

    public static class NormalizeReducer extends Reducer<CareerWritable, ReviewWritable, CareerWritable, ReviewWritable> {
        @Override
        protected void reduce(CareerWritable key, Iterable<ReviewWritable> reviews, Context context)
                throws IOException {
            BufferedReader minMaxReader = getMinMaxBufferedReader();
            String line = minMaxReader.readLine();
            String[] numbersStrings = line.split(" ");
            double minRating = Double.parseDouble(numbersStrings[0]);
            double maxRating = Double.parseDouble(numbersStrings[1]);
        }
    }

    private static boolean isLegalLongitude(double longitude) {
        return longitude >= 8.1461259 && longitude <= 11.1993265;
    }

    private static boolean isLegalLatitude(double latitude) {
        return latitude >= 56.5824856 && latitude <= 57.750511;
    }

    private static BufferedReader getMinMaxBufferedReader() throws IOException {
        InputStream in = minMaxFileSystem.open(minMaxHPath);
        InputStreamReader minMaxHFile = new InputStreamReader(in, StandardCharsets.UTF_8);
        return new BufferedReader(minMaxHFile);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "normalize");
        job.setJarByClass(Normalizer.class);
        job.setMapperClass(NormalizeMapper.class);
//        job.setCombinerClass(SampleReducer.class);
        job.setReducerClass(NormalizeReducer.class);
        job.setMapOutputKeyClass(CareerWritable.class);
        job.setMapOutputValueClass(ReviewWritable.class);
        job.setOutputKeyClass(CareerWritable.class);
        job.setOutputValueClass(ReviewWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        minMaxHPath = new Path(args[2]);
        minMaxFileSystem = minMaxHPath.getFileSystem(conf);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
