package com.acai.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class WeatherMapper extends Mapper<LongWritable, Text, WeatherKey, IntWritable> {

    WeatherKey weatherKey = new WeatherKey();
    IntWritable mVal = new IntWritable();
    public final static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //2019-12-31 00:00:00   36c

        String[] strs = StringUtils.split(value.toString(), '\t');
        LocalDateTime dateTime = LocalDateTime.parse(strs[0],dtf);

        weatherKey.setYear(dateTime.getYear());
        weatherKey.setMonth(dateTime.getMonthValue());
        weatherKey.setDay(dateTime.getDayOfMonth());
        weatherKey.setTemperature(Integer.parseInt(strs[1].substring(0, strs[1].length() - 1)));
        mVal.set(weatherKey.getTemperature());

        context.write(weatherKey, mVal);
    }
}
