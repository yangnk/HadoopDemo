package com.hadoop.demo2.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * @Description: 按年份统计气温降序
 * @Author: ZhOu
 * @Date: 2017/8/13
 */
public class Main {
    public static void main(String[] args) {
        Configuration conf = new Configuration();

        try {
            Job job = Job.getInstance(conf, "Weather");
            job.setJarByClass(Main.class);
            job.setMapperClass(TemperatureMapper.class);
            job.setReducerClass(TemperatureReduce.class);
            job.setMapOutputKeyClass(KeyPair.class);
            job.setMapOutputValueClass(Text.class);
            job.setSortComparatorClass(SortTemperature.class);
            job.setPartitionerClass(DataPartition.class);
            job.setGroupingComparatorClass(GroupTemperature.class);
            job.setNumReduceTasks(3);//设置任务数量

            FileInputFormat.addInputPath(job, new Path("/app/data/hadoop/input"));
            FileOutputFormat.setOutputPath(job, new Path("/app/data/hadoop/output_weather"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
