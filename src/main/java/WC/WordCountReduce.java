package WC;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReduce extends Reducer<Text, IntWritable,Text, IntWritable> {
    //实现父类方法快捷键  ctrl+o
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //定义一个计数器
        int count = 0;
        //累加计数
        for (IntWritable intWritable : values){
            //intWritable转化成int
            count+=intWritable.get();
        }
        //输出
        context.write(key,new IntWritable(count));
    }
}