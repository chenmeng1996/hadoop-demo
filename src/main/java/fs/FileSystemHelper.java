package fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * HDFS工具类
 */
public class FileSystemHelper {

    private String nameNode;
    private String hadoopHome;
    private FileSystem fs;

    public FileSystemHelper() {
        Properties properties = readProperty("/hadoop.properties");
        nameNode = properties.getProperty("hadoop.name-node");
        hadoopHome = properties.getProperty("hadoop.home");

        try {
            //指定hadoop路径
            System.setProperty("hadoop.home.dir", hadoopHome);
            //1.获取文件系统
            Configuration configuration = new Configuration();
            fs = FileSystem.get(new URI(nameNode), configuration);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public FileSystem getFs() {
        return fs;
    }

    public boolean exists(String path) throws Exception {
        Path p = new Path(path);
        return fs.exists(p);
    }


    public void mkdir(String dirName) throws URISyntaxException, IOException {
        //2.执行操作 创建hdfs文件夹
        Path path = new Path(dirName);
        if (!fs.exists(path)) {
            System.out.println("新建hdfs文件夹" + dirName);
            fs.mkdirs(path);
        }
        //关闭资源
        fs.close();
        System.out.println("创建hdfs文件夹结束！");
    }

    /**
     * 本地上传到HDFS
     * @param localFilePath
     * @param dfsFilePath
     * @throws URISyntaxException
     * @throws IOException
     */
    public void uploadFromLocal(String localFilePath, String dfsFilePath) throws URISyntaxException, IOException {
        //2.执行操作 上传文件
        fs.copyFromLocalFile(
                false,
                true,
                new Path(localFilePath),
                new Path(dfsFilePath));
        //关闭资源
        fs.close();
        System.out.println("上传结束！");
    }

    /**
     * HDFS下载到本地
     * @param dfsFilePath
     * @param localPath
     * @throws URISyntaxException
     * @throws IOException
     */
    public void downloadToLocal(String dfsFilePath, String localPath) throws URISyntaxException, IOException {
        //2.执行操作 下载文件
        fs.copyToLocalFile(
                false,
                new Path(dfsFilePath),
                new Path(localPath),
                true);
        //关闭资源
        fs.close();
        System.out.println("下载结束！");
    }

    /**
     * 展示目录下所有文件
     * @param path
     * @throws Exception
     */
    public void list(String path) throws Exception {
        //获取RemoteIterator 得到所有的文件或者文件夹，第一个参数指定遍历的路径，第二个参数表示是否要递归遍历
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(path), true);
        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            System.out.println(fileStatus.getPath().toString());
        }
        fs.close();
    }

    /**
     * 将本地某个文件夹的所有小文件合并成一个大文件，上传到dfs
     * @param localDirPath
     * @throws Exception
     */
    public void uploadMergeFile(String localDirPath, String dfsFilePath) throws Exception {
        FSDataOutputStream os = fs.create(new Path(dfsFilePath));
        //获取本地文件系统
        LocalFileSystem lfs = FileSystem.getLocal(new Configuration());
        //通过本地文件系统获取文件列表，为一个集合
        FileStatus[] fileStatuses = lfs.listStatus(new Path(localDirPath));
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream is = lfs.open(fileStatus.getPath());
            IOUtils.copyBytes(is, os, 4096);
            IOUtils.closeStream(is);
        }
        IOUtils.closeStream(os);
        lfs.close();
        fs.close();
    }

    /**
     * 查看文件内容，输出到标准输出流（控制台）
     * @param filePath
     * @throws Exception
     */
    public void cat(String filePath) throws Exception {
        InputStream in = null;
        try {
            in = fs.open(new Path(filePath));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }

    public void catTwice(String filePath) throws Exception {
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(filePath));
            IOUtils.copyBytes(in, System.out, 4096, false);
            in.seek(0); //回到文件开头位置
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }

    /**
     * 将InputStream保存到HDFS
     * @param inputStream
     * @param dfsFilePath
     * @throws Exception
     */
    public void uploadFromStream(InputStream inputStream, String dfsFilePath) throws Exception {
        InputStream is = new BufferedInputStream(inputStream);
        OutputStream os = fs.create(new Path(dfsFilePath), new Progressable() {
            //写入成功回调函数
            @Override
            public void progress() {
                System.out.println("datanode写入成功");
            }
        });
        IOUtils.copyBytes(is, os, 4096);
    }

    /**
     * 单个文件的文件信息
     * @param filePath
     * @throws Exception
     */
    public void fileStatus(String filePath) throws Exception {
        FileStatus fileStatus = fs.getFileStatus(new Path(filePath));
        System.out.println("文件路径：" + fileStatus.getPath().toString());
        System.out.println("uri：" + fileStatus.getPath().toUri().getPath());
        System.out.println("文件大小：" + fileStatus.getLen());
        System.out.println("修改时间：" + fileStatus.getModificationTime());
        System.out.println("块大小：" + fileStatus.getBlockSize());
        System.out.println("副本数：" + fileStatus.getReplication());
        System.out.println("拥有者：" + fileStatus.getOwner());
        System.out.println("拥有组：" + fileStatus.getGroup());
        System.out.println("权限" + fileStatus.getPermission());
    }

    /**
     * 目录下的内容
     * @param path
     */
    public void listStatus(String... path) throws Exception {
        Path[] paths = new Path[path.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = new Path(path[i]);
        }
        FileStatus[] fileStatuses = fs.listStatus(paths);
        //FileStatus -> Path
        Path[] listedPaths = FileUtil.stat2Paths(fileStatuses);
        for (Path p : listedPaths) {
            System.out.println(p);
        }
    }

    /**
     * 正则查找文件的文件信息，正则规则同Unix bash shell
     * @param pathPattern
     * @throws Exception
     */
    public void globStatus(String pathPattern) throws Exception {
        FileStatus[] fileStatuses = fs.globStatus(new Path(pathPattern));
        //FileStatus -> Path
        Path[] listedPaths = FileUtil.stat2Paths(fileStatuses);
        for (Path p : listedPaths) {
            System.out.println(p);
        }
    }

    /**
     * 删除目录或者文件
     * @param path
     * @throws Exception
     */
    public void delete(String path) throws Exception {
        fs.delete(new Path(path), true);
    }

    /**
     * 文件末尾追加数据
     * @param path
     * @throws Exception
     */
    public void append(String path) throws Exception {
        fs.append(new Path(path));
    }

    public static Properties readProperty(String path) {
        Properties properties = new Properties();
        InputStream inputStream = Object.class.getResourceAsStream(path);
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}
