package 平均值;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text out_key = new Text();
    private final IntWritable out_value = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(" ");

        out_key.set(split[0]);
        out_value.set(Integer.parseInt(split[1]));

        context.write(out_key, out_value);
    }
}
