package common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;

public class CareerWritable implements WritableComparable<CareerWritable> {

    public enum Career {
        PROGRAMMER, TEACHER, WRITER, ACCOUNTANT, MANAGER, DOCTOR, ARTIST, FARMER
    }

    private Career career;

    public CareerWritable() {
    }

    public CareerWritable(Career career) {
        this.career = career;
    }
    
    public Career getCareer() {
        return career;
    }

    public int getCareerDataCount() {
        return CareerWritable.getCareerDataCount(career);
    }

    public static CareerWritable valueOf(String value) {
        return new CareerWritable(Career.valueOf(value));
    }

    public static CareerWritable valueOf(Career career) {
        return new CareerWritable(career);
    }

    public static int getCareerDataCount(Career career) {
        switch (career) {
        case PROGRAMMER:
            return 600149;
        case TEACHER:
            return 607541;
        case WRITER:
            return 606584;
        case ACCOUNTANT:
            return 587367;
        case MANAGER:
            return 595507;
        case DOCTOR:
            return 595815;
        case ARTIST:
            return 590813;
        case FARMER:
            return 600648;
        default:
            throw new IllegalArgumentException();
        }
    }

    public void setCareer(Career career) {
        this.career = career;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(career.ordinal());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        career = Career.values()[in.readInt()];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CareerWritable that = (CareerWritable) o;
        return career == that.career;
    }

    @Override
    public int hashCode() {
        return Objects.hash(career);
    }

    @Override
    public int compareTo(CareerWritable o) {
        if (this == o) return 0;
        return Integer.compare(hashCode(), o.hashCode());
    }

    @Override
    public String toString() {
        return career.ordinal() + "#" + career;
    }
}
