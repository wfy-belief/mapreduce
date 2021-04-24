package com.wfybelief.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private final FlowBean out_value = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        // 遍历集合累加值
        long totalup = 0;
        long totaldown = 0;
        for (FlowBean value : values) {
            totalup += value.getUpFlow();
            totaldown += value.getDownFlow();
        }
        // 封装 key value
        out_value.setUpFlow(totalup);
        out_value.setDownFlow(totaldown);
        out_value.setSumFlow();

        // 写出
        context.write(key, out_value);
    }
}
