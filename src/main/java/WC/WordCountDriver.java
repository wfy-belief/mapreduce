package WC;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //实例化配置文件
        Configuration configuration = new Configuration();
        //定义一个job任务
        Job job = Job.getInstance(configuration);

        //清空路径
        FileSystem  fs = FileSystem.get(configuration);
        Path out_path = new Path("D:\\Hadoop\\project0\\out\\file\\output");
        if (fs.exists(out_path)){
            fs.delete(out_path, true);
        }

        //配置job的信息
        job.setJarByClass(WordCountDriver.class);
        //指定自定义的mapper类以及mapper的输出数据类型到job
        job.setMapperClass(WordCountMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定自定义的reduce以及reduce的输出数据类型（总输出的类型）到job
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //配置输入数据的路径
        FileInputFormat.setInputPaths(job,new Path("D:\\Hadoop\\project0\\in\\file\\input"));
        //配置输出数据的路径
        FileOutputFormat.setOutputPath(job,new Path("D:\\Hadoop\\project0\\out\\file\\output"));

        //提交任务
        job.waitForCompletion(true);
    }
}