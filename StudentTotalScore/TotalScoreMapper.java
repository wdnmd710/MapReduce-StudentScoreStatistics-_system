package StudentScore.StudentTotalScore;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: yuan
 */
public class TotalScoreMapper extends Mapper<Object, Text, IntWritable,Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException { String line = value.toString();
        System.out.println("输入数据：" + line);  // 打印每一行输入数据

        // 跳过表头行
        if (line.startsWith("考号")) {
            System.out.println("跳过表头行：" + line);
            return;
        }

        String[] fields = line.split("\t"); // 使用制表符分隔数据
        if (fields.length < 10) {
            System.out.println("无效数据：" + line);
            return;
        }

        String totalScoreStr = fields[8];  // 获取总分
        try {
            int totalScore = Integer.parseInt(totalScoreStr);  // 转换为整数
            System.out.println("Mapper 输出：总分=" + totalScore + ", 学生信息=" + line);  // 打印输出内容
            context.write(new IntWritable(-totalScore), new Text(line));  // 输出负分数，用以降序排序
        } catch (NumberFormatException e) {
            System.out.println("无效分数：" + totalScoreStr + ", in line: " + line);
        }
    }
}
