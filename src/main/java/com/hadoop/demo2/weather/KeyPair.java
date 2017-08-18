package com.hadoop.demo2.weather;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description: mapper输出封装对象
 * @Author: ZhOu
 * @Date: 2017/8/13
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class KeyPair implements WritableComparable<KeyPair> {
    private int year;
    private int temperature;

    public int compareTo(KeyPair o) {
        int result = Integer.compare(year, o.getYear());
        if (result != 0) {
            return result;
        }
        return Integer.compare(temperature, o.getTemperature());
    }

    /**
     * 序列化
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.year);
        dataOutput.writeInt(this.temperature);
    }

    /**
     * 反序列化
     */
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readInt();
        this.temperature = dataInput.readInt();
    }

    @Override
    public String toString() {
        return year + "\t" + temperature;
    }

    @Override
    public int hashCode() {
        return new Integer(year + temperature).hashCode();
    }
}
