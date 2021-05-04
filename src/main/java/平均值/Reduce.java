package 平均值;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable out_value = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum_ = 0;
        int count_ = 0;
        for (IntWritable value : values) {
            sum_ += value.get();
            count_++;
        }
        out_value.set(sum_ / count_);

        context.write(key, out_value);
    }
}
