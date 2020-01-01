package com.acai.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Weather {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: weather <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "weather");
        job.setJarByClass(Weather.class);

        //----conf----map----
        job.setMapperClass(WeatherMapper.class);
        job.setOutputKeyClass(WeatherKey.class);
        job.setOutputValueClass(IntWritable.class);
        job.setPartitionerClass(WeatherPartitioner.class);
        job.setSortComparatorClass(WeatherKeyComparator.class);
        //分片？
//        job.setCombinerClass(WeatherCombiner.class);

        //----conf----reduce----
        job.setReducerClass(WeatherReducer.class);
        job.setGroupingComparatorClass(WeatherGroupingComparator.class);
        job.setNumReduceTasks(2);


        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
