package HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * 客户端代码常用套路
 * 1、获取客户端对象
 * 2、执行相关的操作命令
 * 3、关闭资源
 * HDFS ZOOKEEPER
 */
public class Client {

    private FileSystem fs;
    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //连接集群nn地址
        URI uri = new URI("file:///D:/Hadoop/project0/in");
        //创建一个配置文件
        Configuration configuration = new Configuration();
        //用户
        String user = "root";
        // 1、获取到客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }
    @Test
    public void test_mkdir() throws IOException {
        //2、创建一个文件夹
        fs.mkdirs(new Path("file:///D:/Hadoop/project0/in/file/test/test"));
    }
    @After
    public void close() throws IOException {
        //3、关闭资源
        fs.close();
    }
}
