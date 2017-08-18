package com.hadoop.demo2.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Description: 自定义分组
 * @Author: ZhOu
 * @Date: 2017/8/13
 */
public class GroupTemperature extends WritableComparator {

    public GroupTemperature() {
        super(KeyPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        KeyPair k1 = (KeyPair) a;
        KeyPair k2 = (KeyPair) b;
        return Integer.compare(k1.getYear(), k2.getYear());
    }
}
