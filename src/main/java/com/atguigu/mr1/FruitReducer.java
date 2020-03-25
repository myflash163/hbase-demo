package com.atguigu.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class FruitReducer extends TableReducer<LongWritable, Text, NullWritable> {
    String cf1 = null;

    @Override
    protected void setup(Context context) {
        Configuration configuration = context.getConfiguration();
        cf1 = configuration.get("cf1");
    }


    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {

            String[] fields = value.toString().split("\t");
            Put put = new Put(Bytes.toBytes(fields[0]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(fields[1]));

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(fields[2]));
            context.write(NullWritable.get(), put);

        }
    }

}
