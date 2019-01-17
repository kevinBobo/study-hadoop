package com.bobo.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author bobo
 * @Description:
 * @date 2019-01-01 19:48
 */
public class FlowJobLocal {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        //封装参数：jar包所在的位置
        job.setJarByClass(FlowJobLocal.class);

        //封装参数：job要调用的mapper实现类和reducer实现类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //封装参数：本次job的mapper实现类产生结果数据的key，value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //封装参数：本次job的reducer实现类产生结果数据的key，value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //封装参数：本次job要处理的输入数据集所在路径
        FileInputFormat.setInputPaths(job, new Path("/Users/bobo/Documents/study-hadoop/study-mapreduce/wordcount/flow/in"));
        //输出路径不存在也可以
        FileOutputFormat.setOutputPath(job, new Path("/Users/bobo/Documents/study-hadoop/study-mapreduce/wordcount/flow/out"));

        //封装参数：想要启动的reduce task数量
        job.setNumReduceTasks(1);

        //提交job给yarn
        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);

    }
}
