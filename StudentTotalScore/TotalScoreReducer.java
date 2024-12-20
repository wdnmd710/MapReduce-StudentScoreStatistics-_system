package StudentScore.StudentTotalScore;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: yuan
 */
public class TotalScoreReducer extends Reducer<IntWritable, Text,Text,IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int actualScore = -key.get();  // 恢复分数为正数
        System.out.println("Reducer 接收到总分：" + actualScore);  // 打印接收到的总分

        for (Text val : values) {
            System.out.println("Reducer 输出：学生信息=" + val.toString() + ", 总分=" + actualScore);  // 打印输出内容
            context.write(val, new IntWritable(actualScore));  // 输出学生信息和实际总分
        }
    }
}
