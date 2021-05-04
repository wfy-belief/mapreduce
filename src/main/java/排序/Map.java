package 排序;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
    private final IntWritable out_key = new IntWritable();
    private final Text out_value = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        get line
        String line = value.toString();
        String[] split = line.split(" ");

//        context write
        out_key.set(Integer.parseInt(split[1]));
        out_value.set(split[0]);
        context.write(out_key, out_value);
    }
}
