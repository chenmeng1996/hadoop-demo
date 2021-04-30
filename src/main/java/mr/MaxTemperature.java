package mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author 陈濛
 * @date 2020/4/16 10:30 下午
 */
public class MaxTemperature {

    /**
     * mapreduce任务
     * @param inputPath 文件输入路径
     * @param outputPath 结果输出路径
     * @throws Exception
     */
    public static void compute(String inputPath, String outputPath) {

        /*运行作业时，要把代码打包成一个jar文件，在Hadoop集群上发布这个文件。
        不必明确指定jar文件的名称，在jobConf对象中传递一个类即可，
        Hadoop利用这个类来查找包含它的jar文件，进而找到相关jar文件。*/
        JobConf jobConf = new JobConf(MaxTemperature.class);
        jobConf.setJobName("Max temperature");

        /*指定输入文件路径，输出文件写入路径*/
        FileInputFormat.addInputPath(jobConf, new Path(inputPath));
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));

        /*Map函数，Reduce函数*/
        jobConf.setMapperClass(MaxTemperatureMapper.class);
        jobConf.setReducerClass(MaxTemperatureReducer.class);
        //combiner函数
        jobConf.setCombinerClass(MaxTemperatureReducer.class);

        /*非必须。当mapper输出和reducer输入类型不一样时，设置mapper输出类型*/
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        try {
            Job job = Job.getInstance(jobConf);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
