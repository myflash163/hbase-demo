package com.atguigu.mr2;

import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Fruit2Driver implements Tool {
    private Configuration configuration = null;

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(configuration);

        job.setJarByClass(Fruit2Driver.class);
        TableMapReduceUtil.initTableMapperJob("fruit",
                new Scan(),
                Fruit2Mapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job);
        TableMapReduceUtil.initTableReducerJob("fruit2",
                Fruit2Reducer.class,
                job);
        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) {

        try {
            //Configuration configuration = new Configuration();
            Configuration configuration = HBaseConfiguration.create();
            ToolRunner.run(configuration, new Fruit2Driver(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
