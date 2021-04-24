package com.wfybelief.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEY IN map 阶段输入的key：偏移量  LongWritable
 * VALUE IN map 阶段输入的value：Text
 * KEY OUT 阶段输出的key：Text
 * VALUE OUT 阶段输出的value：IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text out_key = new Text();
    private final IntWritable out_value = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.获取一行信息
        String line = value.toString();
        //2.切割
        String[] words = line.split(" ");
        //3.循环
        for (String word : words) {
            out_key.set(word);
            context.write(out_key, out_value);
        }
    }
}
