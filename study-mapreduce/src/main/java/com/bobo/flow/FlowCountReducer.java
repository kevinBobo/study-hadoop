package com.bobo.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author bobo
 * @Description:
 * @date 2019-01-02 14:28
 */
public class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int upsum = 0;
        int downsum = 0;
        for (FlowBean flowBean : values) {
            upsum += flowBean.getUpFlow();
            downsum += flowBean.getDownFlow();
        }

        context.write(key,new FlowBean(upsum,downsum,key.toString()));
    }
}
