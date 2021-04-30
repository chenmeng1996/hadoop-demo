package fs;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class FileSystemHelperTest {
    private FileSystemHelper fsHelper = new FileSystemHelper();

    @Test
    public void list() throws Exception {
        fsHelper.list("/");
    }

    @Test
    public void cat() throws Exception {
        fsHelper.cat("hdfs://localhost/test1/hello.txt");
    }

    @Test
    public void catTwice() throws Exception {
        fsHelper.catTwice("hdfs://localhost/test1/hello.txt");
    }

    @Test
    public void download() throws Exception {
        fsHelper.downloadToLocal("hdfs://localhost/test1/hello.txt", "/Users/chenmeng/Downloads");
    }

    @Test
    public void upload() throws Exception {
        fsHelper.uploadFromLocal("/Users/chenmeng/Public/hadoop-test/sample.txt", "hdfs://localhost/user/chenmeng/temperature/sample.txt");
    }

    @Test
    public void fileStatus() throws Exception {
        fsHelper.fileStatus("hdfs://localhost/test1/hello.txt");
    }

    @Test
    void listStatus() throws Exception {
        fsHelper.list("hdfs://localhost/");
    }

    /**
     * 测试 创建没有分配block的新文件的可见性为 立即可见
     * @throws IOException
     */
    @Test
    void testCreate() throws IOException {
        FileSystem fs = fsHelper.getFs();
        //默认写到了当前hdfs的用户目录下，即默认前缀为hdfs://user/chenmeng/
        Path path1 = new Path("p");
        fs.create(path1);
//        Assertions.assertTrue(fs.exists(path1));
    }

    /**
     * 写入内容不保证立即可见，即使数据流已经刷新并存储。
     * flush只保证数据写入到客户端所在的系统内存
     * @throws IOException
     */
    @Test
    void flush() throws IOException {
        FileSystem fs = fsHelper.getFs();
        Path path = new Path("p1");
        FSDataOutputStream outputStream = fs.create(path);
        outputStream.write("content".getBytes(StandardCharsets.UTF_8));
        outputStream.flush();
//        Assertions.assertEquals(fs.getFileStatus(path).getLen(), 0);
    }

    /**
     * 写入内容保证立即可见，强行将所有缓存刷新到所有datanode。
     * hflush只保证写入到datanode内存，hsync保证写入到datanode磁盘。
     * @throws IOException
     */
    @Test
    void hFlush() throws IOException {
        FileSystem fs = fsHelper.getFs();
        Path path = new Path("p2");
        FSDataOutputStream outputStream = fs.create(path);
        outputStream.write("content".getBytes(StandardCharsets.UTF_8));
        outputStream.hflush();
//        Assertions.assertEquals(fs.getFileStatus(path).getLen(), "content".length());
    }

    /**
     * 测试文件立即刷新到本地磁盘
     * @throws Exception
     */
    @Test
    void fsync() throws Exception {
        String localfile = "./test.txt";
        File file = new File(localfile);
        FileOutputStream outputStream = new FileOutputStream(file);
        outputStream.write("content".getBytes(StandardCharsets.UTF_8));
        //更新的数据 立即刷新到磁盘
        outputStream.getFD().sync();
//        Assertions.assertEquals(file.length(), "content".length());
    }
}
