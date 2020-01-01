package com.acai.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WeatherKeyComparator extends WritableComparator {
    public WeatherKeyComparator() {
        super(WeatherKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WeatherKey keyA = (WeatherKey) a;
        WeatherKey keyB = (WeatherKey) b;

        int c1 = Integer.compare(keyA.getYear(), keyB.getYear());
        if (c1 == 0) {
            int c2 = Integer.compare(keyA.getMonth(), keyB.getMonth());
            if (c2 == 0) {
                int c3 = Integer.compare(keyB.getTemperature(), keyA.getTemperature());
                return c3;
            }
            return c2;
        }
        return c1;
    }
}
