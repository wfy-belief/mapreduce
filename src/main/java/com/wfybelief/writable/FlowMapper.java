package com.wfybelief.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private final FlowBean out_value = new FlowBean();
    private final Text out_key = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // line
        String line = value.toString();

        // split
        String[] split = line.split("\t");

        // get data
        String phone = split[1];
        String up = split[split.length - 3];
        String down = split[split.length - 2];

        out_key.set(phone);
        out_value.setUpFlow(Long.parseLong(up));
        out_value.setDownFlow(Long.parseLong(down));
        out_value.setSumFlow();

        context.write(out_key, out_value);
    }
}
