package 排序;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
    private final Text out_value = new Text();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            out_value.set(value);
            context.write(key, out_value);
        }
    }
}
