package com.wfybelief.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*KEY IN reduce 阶段输入的key： Text
 *VALUE IN reduce 阶段输入的value：IntWritable
 *KEY OUT reduce 阶段输出的key：Text
 *VALUE OUT reduce 阶段输出的value：IntWritable
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable out_value = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        out_value.set(sum);
        context.write(key, out_value);
    }
}
