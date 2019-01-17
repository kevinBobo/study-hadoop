package com.bobo.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author bobo
 * @Description:
 * @date 2019-01-02 14:24
 */
public class FlowCountMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();
        String[] values = text.split("\t");

        String phone = values[1];

        int upFLow = Integer.parseInt(values[values.length-3]);
        int downFlow = Integer.parseInt(values[values.length-2]);

        context.write(new Text(phone),new FlowBean(upFLow,downFlow,phone));
    }


}
