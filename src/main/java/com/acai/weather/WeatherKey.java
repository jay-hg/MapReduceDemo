package com.acai.weather;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class WeatherKey implements WritableComparable<WeatherKey> {
    private int year;
    private int month;
    private int day;
    private int temperature;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    @Override
    public int compareTo(WeatherKey that) {
        // 约定的 日期正序
        int c1 = Integer.compare(this.getYear(), that.getYear());
        if (c1 == 0) {
            int c2 = Integer.compare(this.getMonth(), that.getMonth());
            if (c2 == 0) {
                int c3 = Integer.compare(this.getDay(), that.getDay());
                return c3;
            }
            return c2;
        }
        return c1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(month);
        dataOutput.writeInt(day);
        dataOutput.writeInt(temperature);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readInt();
        month = dataInput.readInt();
        day = dataInput.readInt();
        temperature = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WeatherKey that = (WeatherKey) o;
        return year == that.year &&
                month == that.month &&
                day == that.day &&
                temperature == that.temperature;
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, month, day, temperature);
    }
}
