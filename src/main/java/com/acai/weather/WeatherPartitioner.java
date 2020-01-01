package com.acai.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class WeatherPartitioner extends Partitioner<WeatherKey, IntWritable> {
    @Override
    public int getPartition(WeatherKey weatherKey, IntWritable intWritable, int numPartitions) {
        return (weatherKey.getYear()+weatherKey.getMonth())%numPartitions;
    }
}
