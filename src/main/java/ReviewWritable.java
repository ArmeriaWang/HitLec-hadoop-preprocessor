import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ReviewWritable implements Writable, Cloneable {
    private final Text reviewId;
    private final DoubleWritable longitude;
    private final DoubleWritable latitude;
    private final DoubleWritable altitude;
    private final Text reviewDate;
    private final Text temperature;
    private final DoubleWritable rating;
    private final Text userId;
    private final Text userBirthday;
    private final Text userNationality;
    private final CareerWritable userCareer;
    private final DoubleWritable userIncome;
    private boolean vacantRating;
    private boolean vacantUserIncome;

    public ReviewWritable() {
        reviewId = new Text();
        longitude = new DoubleWritable();
        latitude = new DoubleWritable();
        altitude = new DoubleWritable();
        reviewDate = new Text();
        temperature = new Text();
        rating = new DoubleWritable();
        userId = new Text();
        userBirthday = new Text();
        userNationality = new Text();
        userCareer = new CareerWritable();
        userIncome = new DoubleWritable();
        vacantRating = true;
        vacantUserIncome = true;
    }

    public ReviewWritable(String rawString) {
        String[] elements = rawString.split("\\|");
        this.reviewId = new Text(elements[0]);
        this.longitude = new DoubleWritable(Double.parseDouble(elements[1]));
        this.latitude = new DoubleWritable(Double.parseDouble(elements[2]));
        this.altitude = new DoubleWritable(Double.parseDouble(elements[3]));
        this.reviewDate = new Text(elements[4]);
        this.temperature = new Text(elements[5]);
        this.rating = new DoubleWritable();
        if (!(vacantRating = elements[6].equals("?"))) {
            this.rating.set(Double.parseDouble(elements[6]));
        }
        this.userId = new Text(elements[7]);
        this.userBirthday = new Text(elements[8]);
        this.userNationality = new Text(elements[9]);
        this.userCareer = CareerWritable.valueOf(elements[10].toUpperCase());
        this.userIncome = new DoubleWritable();
        if (!(vacantUserIncome = elements[11].equals("?"))) {
            this.userIncome.set(Double.parseDouble(elements[11]));
        }
    }

    private static void copyFields(ReviewWritable dest, ReviewWritable src) {
        dest.reviewId.set(src.getReviewId());
        dest.longitude.set(src.getLongitude());
        dest.latitude.set(src.getLatitude());
        dest.altitude.set(src.getAltitude());
        dest.reviewDate.set(src.getReviewDate());
        dest.temperature.set(src.getTemperature());
        if (!(dest.vacantRating = src.vacantRating)) {
            dest.rating.set(src.getRating());
        }
        dest.userId.set(src.getUserId());
        dest.userBirthday.set(src.getUserBirthday());
        dest.userNationality.set(src.getUserNationality());
        dest.userCareer.setCareer(src.getUserCareer());
        if (!(dest.vacantUserIncome = src.vacantUserIncome)) {
            dest.userIncome.set(src.getUserIncome());
        }
    }

    @Override
    public ReviewWritable clone() {
        try {
            super.clone();
        }
        catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        ReviewWritable that = new ReviewWritable();
        copyFields(that, this);
        return that;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ReviewWritable that = new ReviewWritable(Text.readString(in));
        copyFields(this, that);
    }

    public String getReviewId() {
        return reviewId.toString();
    }

    public double getLongitude() {
        return longitude.get();
    }

    public double getLatitude() {
        return latitude.get();
    }

    public double getAltitude() {
        return altitude.get();
    }

    public String getReviewDate() {
        return reviewDate.toString();
    }

    public String getTemperature() {
        return temperature.toString();
    }

    public String getUserId() {
        return userId.toString();
    }

    public String getUserBirthday() {
        return userBirthday.toString();
    }

    public String getUserNationality() {
        return userNationality.toString();
    }

    public CareerWritable.Career getUserCareer() {
        return userCareer.getCareer();
    }

    public double getRating() {
        if (vacantRating) {
            throw new RuntimeException("Visiting the rating of an review instance with vacant rating");
        }
        return rating.get();
    }

    public double getUserIncome() {
        if (vacantUserIncome) {
            throw new RuntimeException("Visiting the userIncome of an review instance with vacant userIncome");
        }
        return userIncome.get();
    }

    public boolean isVacantRating() {
        return vacantRating;
    }

    public boolean isVacantUserIncome() {
        return vacantUserIncome;
    }

    public void setRating(double rating) {
        this.rating.set(rating);
    }

    public void setReviewDate(String reviewDate) {
        this.reviewDate.set(reviewDate);
    }

    public void setUserBirthday(String userBirthday) {
        this.userBirthday.set(userBirthday);
    }

    public void setTemperature(String temperature) {
        this.temperature.set(temperature);
    }

    @Override
    public String toString() {
        String ratingString = vacantRating ? "?" : String.format("%.6f", rating.get());
        String userIncomeString = vacantUserIncome ? "?" : String.format("%.1f", userIncome.get());
        return reviewId + "|" + longitude + "|" + latitude + "|" + altitude + "|" +
                reviewDate + "|" + temperature + "|" + ratingString + "|" +
                userId + "|" + userBirthday + "|" + userNationality + "|" +
                userCareer.getCareer().toString() + "|" + userIncomeString;
    }

}