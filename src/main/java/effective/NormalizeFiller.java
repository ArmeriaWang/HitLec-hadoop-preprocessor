package effective;

import common.CareerWritable;
import common.ReviewWritable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.util.*;

public class NormalizeFiller {

    private static Path minMaxHPath;
    private static FileSystem minMaxFileSystem;
    private final static Random random = new Random();

    public static class NormalizeFillerMapper extends Mapper<Object, Text, IntWritable, ReviewWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String rawString = itr.nextToken();
                ReviewWritable review = new ReviewWritable(rawString);
                context.write(new IntWritable(random.nextInt()), review);
            }
        }
    }

    public static class NormalizeFillerReducer
            extends Reducer<IntWritable, ReviewWritable, NullWritable, ReviewWritable> {

        private double minRating;
        private double maxRating;
        private double minUserIncome;
        private double maxUserIncome;
        private double minLatitude;
        private double maxLatitude;
        private double minLongitude;
        private double maxLongitude;
        private double minAltitude;
        private double maxAltitude;

        private final int len = 5;
        private final double[] w = new double[len];
        private final double learningRate = 0.004;
        private final Set<ReviewWritable> vacantRatingReviews = new HashSet<>();
        private final Set<ReviewWritable> vacantUserIncomeReviews = new HashSet<>();
        private final Map<Pair<String, CareerWritable.Career>, Pair<Double, Integer>> userIncomeStats = new HashMap<>();
        private double incomeSumAll;
        private int incomeStatsCnt;
        private double deltaSum;
        private int printInterval;

        private double getDoubleFromMinMax(BufferedReader reader) throws IOException {
            String line = reader.readLine();
            return Double.parseDouble(line);
        }

        @Override
        protected void setup(Context context) throws IOException {
            InputStream in = minMaxFileSystem.open(minMaxHPath);
            InputStreamReader inputStreamReader = new InputStreamReader(in, StandardCharsets.UTF_8);
            BufferedReader minMaxReader = new BufferedReader(inputStreamReader);
            minRating = getDoubleFromMinMax(minMaxReader);
            maxRating = getDoubleFromMinMax(minMaxReader);
            minUserIncome = getDoubleFromMinMax(minMaxReader);
            maxUserIncome = getDoubleFromMinMax(minMaxReader);
            minAltitude = getDoubleFromMinMax(minMaxReader);
            maxAltitude = getDoubleFromMinMax(minMaxReader);
            minLatitude = getDoubleFromMinMax(minMaxReader);
            maxLatitude = getDoubleFromMinMax(minMaxReader);
            minLongitude = getDoubleFromMinMax(minMaxReader);
            maxLongitude = getDoubleFromMinMax(minMaxReader);

            for (int i = 0; i < len; i++) {
                w[i] = Math.random();
            }
            incomeSumAll = 0.0;
            incomeStatsCnt = 0;
            deltaSum = 0.0;
            printInterval = 50;
            vacantUserIncomeReviews.clear();
            vacantRatingReviews.clear();
            userIncomeStats.clear();
        }

        @Override
        protected void reduce(IntWritable key, Iterable<ReviewWritable> reviews, Context context)
                throws IOException, InterruptedException {
            for (ReviewWritable reviewFromIter : reviews) {
                ReviewWritable review = reviewFromIter.clone();
                if (!review.isVacantRating()) {
                    review.setRating(normalizeDouble(reviewFromIter.getRating(), minRating, maxRating));
                }
                review.setReviewDate(normalizeDate(reviewFromIter.getReviewDate()));
                review.setUserBirthday(normalizeDate(reviewFromIter.getUserBirthday()));
                review.setTemperature(normalizeTemperature(reviewFromIter.getTemperature()));
                double[] wPre = new double[len];

                System.arraycopy(w, 0, wPre, 0, len);
                if (review.isVacantUserIncome()) {
                    vacantUserIncomeReviews.add(review.clone());
                    continue;
                } else if (review.isVacantRating()) {
                    vacantRatingReviews.add(review.clone());
                    continue;
                }
                Pair<String, CareerWritable.Career> nationalityCareer =
                        new ImmutablePair<>(review.getUserNationality(), review.getUserCareer());
                Pair<Double, Integer> originalStats =
                        userIncomeStats.getOrDefault(nationalityCareer, new ImmutablePair<>(0.0, 0));
                userIncomeStats.put(nationalityCareer,
                        new ImmutablePair<>(originalStats.getLeft() + review.getUserIncome(),
                                originalStats.getRight() + 1));
                context.write(NullWritable.get(), review);
                double[] x = getParameters(review);
                double delta = review.getRating() - getProduct(x, wPre);
                deltaSum += Math.abs(delta);
                incomeSumAll += review.getUserIncome();
                incomeStatsCnt++;
                for (int j = 0; j < len; j++) {
                    w[j] = w[j] + learningRate * delta * x[j];
                }
                if (incomeStatsCnt % printInterval == 0) {
                    System.out.println(deltaSum / printInterval);
                    deltaSum = 0;
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (ReviewWritable review : vacantUserIncomeReviews) {
                Pair<String, CareerWritable.Career> nationalityCareer =
                        new ImmutablePair<>(review.getUserNationality(), review.getUserCareer());
                Pair<Double, Integer> statsResult = userIncomeStats.getOrDefault(nationalityCareer,
                        new ImmutablePair<>(incomeSumAll, incomeStatsCnt));
                review.setUserIncome(statsResult.getLeft() / statsResult.getRight());
                context.write(NullWritable.get(), review);
            }
            for (ReviewWritable review : vacantRatingReviews) {
                double[] x = getParameters(review);
                review.setRating(getProduct(x, w));
                context.write(NullWritable.get(), review);
            }
        }

        private static double normalizeDouble(double value, double minValue, double maxValue) {
            if (maxValue - minValue < 1e-9) {
                return 0;
            }
            return (value - minValue) / (maxValue - minValue);
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

        private double getProduct(double[] v1, double[] v2) {
            double ret = 0;
            for (int i = 0; i < len; i++) {
                ret += v1[i] * v2[i];
            }
            return ret;
        }

        private double[] getParameters(ReviewWritable review) {
            double[] ret = new double[len];
            ret[0] = normalizeDouble(review.getUserIncome(), minUserIncome, maxUserIncome);
            ret[1] = normalizeDouble(review.getLatitude(), minLatitude, maxLatitude);
            ret[2] = normalizeDouble(review.getLongitude(), minLongitude, maxLongitude);
            ret[3] = normalizeDouble(review.getAltitude(), minAltitude, maxAltitude);
            ret[4] = 1;
            return ret;
        }

        private String vector2String(double[] v) {
            StringBuilder builder = new StringBuilder("{");
            for (int i = 0; i < len; i++) {
                builder.append(String.format("%.3f", v[i]));
                if (i < len - 1) {
                    builder.append(", ");
                }
            }
            builder.append("}");
            return builder.toString();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "normalize");
        job.setJarByClass(NormalizeFiller.class);
        job.setMapperClass(NormalizeFillerMapper.class);
        job.setReducerClass(NormalizeFillerReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
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
