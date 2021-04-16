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

    /* public Review(String reviewId, double longitude, double latitude, double altitude, String reviewDate,
            String temperature, double rating, String userId, String userBirthday, String userNationality,
            Career userCareer, double userIncome) {
        this.reviewId = reviewId;
        this.longitude = longitude;
        this.latitude = latitude;
        this.altitude = altitude;
        this.reviewDate = reviewDate;
        this.temperature = temperature;
        this.rating = rating;
        this.userId = userId;
        this.userBirthday = userBirthday;
        this.userNationality = userNationality;
        this.userCareer = userCareer;
        this.userIncome = userIncome;
    } */

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
    }

    public ReviewWritable(String rawString) {
        String[] elements = rawString.split("\\|");
        this.reviewId = new Text(elements[0]);
        this.longitude = new DoubleWritable(Double.parseDouble(elements[1]));
        this.latitude = new DoubleWritable(Double.parseDouble(elements[2]));
        this.altitude = new DoubleWritable(Double.parseDouble(elements[3]));
        this.reviewDate = new Text(elements[4]);
        this.temperature = new Text(elements[5]);
        this.rating = new DoubleWritable(elements[6].equals("?") ? -1.0 : Double.parseDouble(elements[6]));
        this.userId = new Text(elements[7]);
        this.userBirthday = new Text(elements[8]);
        this.userNationality = new Text(elements[9]);
        this.userCareer = CareerWritable.valueOf(elements[10].toUpperCase());
        this.userIncome = new DoubleWritable(elements[11].equals("?") ? -1.0 : Double.parseDouble(elements[11]));
    }

    @Override
    public ReviewWritable clone() {
        try {
            super.clone();
        }
        catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return new ReviewWritable(toString());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.reviewId.write(out);
        this.longitude.write(out);
        this.latitude.write(out);
        this.altitude.write(out);
        this.reviewDate.write(out);
        this.temperature.write(out);
        this.rating.write(out);
        this.userId.write(out);
        this.userBirthday.write(out);
        this.userNationality.write(out);
        this.userCareer.write(out);
        this.userIncome.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.reviewId.readFields(in);
        this.longitude.readFields(in);
        this.latitude.readFields(in);
        this.altitude.readFields(in);
        this.reviewDate.readFields(in);
        this.temperature.readFields(in);
        this.rating.readFields(in);
        this.userId.readFields(in);
        this.userBirthday.readFields(in);
        this.userNationality.readFields(in);
        this.userCareer.readFields(in);
        this.userIncome.readFields(in);
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

    public double getRating() {
        return rating.get();
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

    public double getUserIncome() {
        return userIncome.get();
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

    @Override
    public String toString() {
        return reviewId + "|" + longitude + "|" + latitude + "|" + altitude + "|" +
                reviewDate + "|" + temperature + "|" + rating + "|" +
                userId + "|" + userBirthday + "|" + userNationality + "|" +
                userCareer.getCareer().toString() + "|" + userIncome;
    }

}