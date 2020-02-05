package com.acai.friends;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FriendMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable zero = new IntWritable(0);
    private Text reduceKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Tom Cat Hello Hadoop Spring
        String[] strs = value.toString().split(" ");
        for (int i = 1; i < strs.length; i++) {
            String uniqueKey = uniqueKey(strs[0], strs[i]);
            reduceKey.set(uniqueKey);
            context.write(reduceKey,zero);

            for (int j = i+1; j < strs.length; j++) {
                uniqueKey = uniqueKey(strs[i], strs[j]);
                reduceKey.set(uniqueKey);
                context.write(reduceKey,one);
            }
        }
    }

    private String uniqueKey(String str1, String str2) {
        if (str2.compareTo(str1) < 0) {
            return str2 + " " + str1;
        }
        return str1 + " " + str2;
    }


}
