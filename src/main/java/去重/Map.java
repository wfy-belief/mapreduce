package 去重;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private  final Text out_key = new Text();
    private  final IntWritable out_value = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // get line
        String line = value.toString();
        String[] split = line.split(" ");
        out_key.set(split[0]);
        // set out put
        context.write(out_key, out_value);
    }
}
