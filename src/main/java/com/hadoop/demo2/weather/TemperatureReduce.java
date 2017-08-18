package com.hadoop.demo2.weather;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description:
 * @Author: ZhOu
 * @Date: 2017/8/13
 */
public class TemperatureReduce extends Reducer<KeyPair, Text, KeyPair, Text> {
    @Override
    protected void reduce(KeyPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text v : values) {
            context.write(key, v);
        }
    }
}
