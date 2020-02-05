package com.acai;

import com.acai.friends.FriendMapper;
import com.acai.friends.FriendReducer;
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

public class FriendMRTest {
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    private final IntWritable zero = new IntWritable(0);
    private final IntWritable one = new IntWritable(1);
    @Before
    public void setUp() {
        //测试mapreduce
        FriendMapper mapper = new FriendMapper();
        FriendReducer reducer = new FriendReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
//        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    //测试mapper
    @Test
    public void testMapper() throws IOException {
        List<Pair<Text, IntWritable>> outputRecords = new ArrayList<>();
        outputRecords.add(new Pair<Text, IntWritable>(new Text("Cat Tom"),zero));
        outputRecords.add(new Pair<Text, IntWritable>(new Text("Cat Hello"),one));
        outputRecords.add(new Pair<Text, IntWritable>(new Text("Hello Tom"),zero));

        mapDriver.withInput(new LongWritable(), new Text(
                "Tom Cat Hello"));
        mapDriver.withAllOutput(outputRecords);
        mapDriver.runTest();
    }
}
