package com.hadoop.demo2.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Description: 自定义排序
 * @Author: ZhOu
 * @Date: 2017/8/13
 */
public class SortTemperature extends WritableComparator {

    public SortTemperature() {
        super(KeyPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        KeyPair k1 = (KeyPair) a;
        KeyPair k2 = (KeyPair) b;
        int result = Integer.compare(k1.getYear(), k2.getYear());
        if (result != 0) {
            return result;
        }
        return Integer.compare(k2.getTemperature(), k1.getTemperature());
    }
}
