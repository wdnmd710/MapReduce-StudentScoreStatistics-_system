package StudentScore.Top100;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: yuan
 */
public class Top100Mapper extends Mapper<Object, Text,Text, Text> {
    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        // 跳过表头行
        if (line.startsWith("考号")) {
            return;
        }

        // 假设数据用制表符分隔，按顺序分别是：考号、姓名、语文、数学、英语、物理、化学、生物、总分、省份
        String[] fields = line.split("\t");
        if (fields.length < 10) {
            return; // 无效数据行
        }
        // 获取考号、姓名
        String examId = fields[0];
        String name = fields[1];
        // 获取各科成绩
        String studentInfo = fields[0] + "\t" + fields[1]; // 考号和姓名
        try {
            // 语文
            int chineseScore = Integer.parseInt(fields[2]);
            context.write(new Text("语文"), new Text(examId + "\t" + name + "\t" +chineseScore));

            // 数学
            int mathScore = Integer.parseInt(fields[3]);
            context.write(new Text("数学"), new Text(examId + "\t" + name + "\t" +mathScore));

            // 英语
            int englishScore = Integer.parseInt(fields[4]);
            context.write(new Text("英语"), new Text(examId + "\t" + name + "\t" +englishScore));

            //物理
            int physicsScore = Integer.parseInt(fields[5]);
            context.write(new Text("物理"), new Text(examId + "\t" + name + "\t" +physicsScore));

            //政治
            int zzScore = Integer.parseInt(fields[6]);
            context.write(new Text("政治"), new Text(examId + "\t" + name + "\t" +zzScore));

            //生物
            int biologyScore = Integer.parseInt(fields[7]);
            context.write(new Text("生物"), new Text(examId + "\t" + name + "\t" +biologyScore));

        } catch (NumberFormatException e) {
            // 处理数据格式异常
            System.err.println("无效的分数: " + e.getMessage());
        }
    }
}
