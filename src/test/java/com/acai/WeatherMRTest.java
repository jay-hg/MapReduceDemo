package com.acai;

import com.acai.weather.WeatherKey;
import com.acai.weather.WeatherMapper;
import com.acai.weather.WeatherReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WeatherMRTest {
    MapDriver<LongWritable, Text, WeatherKey, IntWritable> mapDriver;
    ReduceDriver<WeatherKey, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        //测试mapreduce
        WeatherMapper mapper = new WeatherMapper();
        WeatherReducer reducer = new WeatherReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
//        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    //测试mapper
    @Test
    public void testMapper() throws IOException {
        List<Pair<WeatherKey, IntWritable>> outputRecords = new ArrayList<>();
        WeatherKey weatherKey = new WeatherKey();
        weatherKey.setYear(1949);
        weatherKey.setMonth(10);
        weatherKey.setDay(1);
        weatherKey.setTemperature(34);
        outputRecords.add(new Pair<WeatherKey, IntWritable>(weatherKey, new IntWritable(weatherKey.getTemperature())));

        mapDriver.withInput(new LongWritable(), new Text(
                "1949-10-01 14:21:02\t34c"));
        mapDriver.withAllOutput(outputRecords);
        mapDriver.runTest();
    }
}
