package com.hadoop.demo2.weather;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Description: 自定义分区:不同年份的数据分到不同的地方
 * @Author: ZhOu
 * @Date: 2017/8/13
 */
public class DataPartition extends Partitioner<KeyPair, Text> {

    /**
     * 数据分区
     *
     * @param key   自定义的数据key
     * @param value 温度
     * @param num   分区个数
     * @return 分区文件下标
     */
    @Override
    public int getPartition(KeyPair key, Text value, int num) {
        return (key.getYear() * 100) % num;
    }
}
