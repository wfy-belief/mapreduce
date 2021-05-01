package 去重;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.实例化配置文件
        Configuration configuration = new Configuration();
        //2.定义一个job任务
        Job job = Job.getInstance(configuration);
        //清空路径

        //3.配置job的信息
        job.setJarByClass(Driver.class);
        //4.指定自定义的mapper类以及mapper的输出数据类型到job
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //指定自定义的reduce以及reduce的输出数据类型（总输出的类型）到job
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //配置输入数据的路径
        FileInputFormat.setInputPaths(job, new Path("D:\\Hadoop\\project0\\in\\file\\in2"));
        //配置输出数据的路径
        FileOutputFormat.setOutputPath(job, new Path("D:\\Hadoop\\project0\\out\\file\\out2"));
        //提交任务
        job.waitForCompletion(true);
    }
}
