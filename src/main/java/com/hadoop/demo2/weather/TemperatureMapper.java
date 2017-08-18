package com.hadoop.demo2.weather;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDate;

/**
 * @Description: 自定义mapper
 * @Author: ZhOu
 * @Date: 2017/8/13
 */
public class TemperatureMapper extends Mapper<LongWritable, Text, KeyPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();//读取每行数据
        String[] str = line.split(" ");//分割数据
        if (str[0] != null) {
            System.out.println(str[0]);
            int year = LocalDate.parse(str[0]).getYear();
            int temperature = Integer.parseInt(str[2].substring(0, str[2].lastIndexOf("℃")));
            context.write(new KeyPair(year, temperature), value);
        }
    }
}
