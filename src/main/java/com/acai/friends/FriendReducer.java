package com.acai.friends;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FriendReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        boolean isFriend = false;
        int commonFriends = 0;
        for (IntWritable val : values) {
            if (val.get() == 0) {
                isFriend = true;
                break;
            } else {
                commonFriends += val.get();
            }
        }

        if (!isFriend) {
            context.write(key,new IntWritable(commonFriends));
        }
    }
}
