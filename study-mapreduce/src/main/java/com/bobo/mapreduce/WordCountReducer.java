package com.bobo.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author bobo
 * @Description:
 * @date 2019-01-01 18:54
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        AtomicInteger count = new AtomicInteger(0);
        Iterator<IntWritable> iterator = values.iterator();
        iterator.forEachRemaining(value-> count.addAndGet(value.get()));
        context.write(key,new IntWritable(count.intValue()));
    }
}
