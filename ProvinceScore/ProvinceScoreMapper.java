package StudentScore.ProvinceScore;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: yuan
 */
public class ProvinceScoreMapper extends Mapper<Object, Text,Text,Text> {
    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        System.out.println("输入数据：" + line);

        // Skip the header line
        if (line.startsWith("考号")) {
            System.out.println("跳过表头行：" + line);
            return;
        }

        String[] fields = line.split("\t"); // 使用制表符分隔数据
        if (fields.length < 10) {
            System.out.println("无效数据：" + line);
            return;
        }

        String province = fields[9]; // 获取省份
        String totalScore = fields[8]; // 获取总分

        System.out.println("输出数据：" + province + "," + totalScore + "," + line);
        context.write(new Text(province), new Text(totalScore + "," + line));
    }
}
