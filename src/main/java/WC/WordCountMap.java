package WC;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMap extends Mapper<LongWritable, Text,Text, IntWritable> {
    //实现父类的快捷键 alt+insert（INS）
    private final Text out_key = new Text();
    private final IntWritable out_value = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        String line = value.toString();
        //切分数据，按照空格切分
        String[] fields = line.split(" ");
        //遍历获取每个单词
        for (String field : fields){
            //输出,每个单词拼接 1（标记） （java（K） 1（V））
            out_key.set(field);
            context.write(out_key,out_value);
        }

    }
}