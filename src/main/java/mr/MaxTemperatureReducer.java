package mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author 陈濛
 * @date 2020/4/16 10:15 下午
 */
public class MaxTemperatureReducer extends MapReduceBase
        implements Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int maxValue = Integer.MIN_VALUE;
        while (iterator.hasNext()) {
            maxValue = Math.max(maxValue, iterator.next().get());
        }
        outputCollector.collect(text, new IntWritable(maxValue));
    }
}
