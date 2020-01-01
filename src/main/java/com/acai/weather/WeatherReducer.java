package com.acai.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeatherReducer extends Reducer<WeatherKey, IntWritable, Text, IntWritable> {

    //相同的key为一组，调用一次reduce方法
    //2019-12-31 33c
    //2019-12-30 28c
    //这两组通过groupingComparator认定为key相同

    Text rKey = new Text();
    IntWritable rVal = new IntWritable();

    @Override
    protected void reduce(WeatherKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int flag = 0;
        int day = 0;
        for (IntWritable v : values) {
            if (flag == 0) {
                rKey.set(key.getYear() + "-" + key.getMonth() + ":" + key.getTemperature());
                rVal.set(key.getTemperature());
                flag ++;
                day = key.getDay();
                context.write(rKey,rVal);
            }
            if (flag != 0 && day != key.getDay()) {
                rKey.set(key.getYear() + "-" + key.getMonth() + ":" + key.getTemperature());
                rVal.set(key.getTemperature());
                context.write(rKey,rVal);
                break;
            }

        }
    }
}
