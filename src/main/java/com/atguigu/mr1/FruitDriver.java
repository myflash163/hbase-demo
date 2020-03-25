package com.atguigu.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FruitDriver implements Tool {
    private Configuration configuration = null;

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(FruitDriver.class);

        job.setMapperClass(FruitMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        //设置Reducer类
        TableMapReduceUtil.initTableReducerJob(args[1], FruitReducer.class, job);
        //设置输入参数
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //提交任务
        boolean result = job.waitForCompletion(true);

        return result ? 0 : 1;
    }

    public void setConf(Configuration configuration) {

        this.configuration = configuration;
    }

    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            int run = ToolRunner.run(configuration, new FruitDriver(), args);
            System.exit(run);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
