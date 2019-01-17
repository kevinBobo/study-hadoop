package com.bobo.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN:是map task读取到的数据key类型，是一行的起始偏移量long
 * VALUEIN：是 map task读取到的数据value类型，是一行的内容string
 *
 * KEYOUT：是用户的自定义map方法要返回的结果kv数据的key的类型，在wordcount逻辑中，我们要返回的是单词的string
 * VALUEOUT：是用户的自定义map方法要返回的结果kv数据的value类型，在wordcount逻辑中我们要返回的是中暑integer
 *
 * 但是，在mapreduce中，map产生的数据需要传输给reduce，需要进行序列化和反序列化，而jdk中的原声序列化机制产生的数据量比较冗余，
 * 就会导致数据在mapreduce运行过程中传输效率低下
 * 所以，hadoop专门设计了自己的序列化机制，那么，maoreduce中传输的类型就必须实现hadoop自己的序列化接口
 *
 * hadoop为jdk中常用的基本类型Long，String，Integer，Float等数据类型封装了自己的实现了hadoop序列化的接口类型：
 * LongWritable，Text，IntWritable，FloatWritable
 *
 * @author bobo
 * @Description:
 * @date 2019-01-01 18:38
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();
        String[] words = text.split(" ");
        for (String word :words) {
            context.write(new Text(word),new IntWritable(1));
        }
    }

}
