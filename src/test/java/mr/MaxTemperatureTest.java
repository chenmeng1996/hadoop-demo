package mr;

import fs.FileSystemHelper;
import org.junit.jupiter.api.Test;


/**
 * @author 陈濛
 * @date 2020/4/17 4:10 下午
 */
public class MaxTemperatureTest {

    private FileSystemHelper fsHelper = new FileSystemHelper();

    @Test
    public void testComputeLocalFile() {
        String inputPath = "file:///Users/chenmeng/Public/hadoop-test/sample.txt";
        String outputPath = "file:///Users/chenmeng/Public/hadoop-test/output";
        MaxTemperature.compute(inputPath, outputPath);
    }

    @Test
    public void testComputeHDFSFile() throws Exception {
        //上传本地文件到HDFS
        String local = "/Users/chenmeng/Public/hadoop-test/sample.txt";
        String hdfsInput = "hdfs://localhost/user/chenmeng/temperature/sample.txt";
        String hdfsOutput = "hdfs://localhost/user/chenmeng/temperature/output";
        if (!fsHelper.exists(hdfsInput)) {
            fsHelper.uploadFromLocal(local, hdfsInput);
        }
        MaxTemperature.compute(hdfsInput, hdfsOutput);
    }
}
