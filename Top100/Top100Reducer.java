package StudentScore.Top100;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * @Author: yuan
 */
public class Top100Reducer extends Reducer<Text, Text,Text,Text> {
    // 定义每个科目的前100名成绩，用 TreeMap 来存储
    private TreeMap<Integer, List<String>> top100 = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
// 清空 TreeMap
        top100.clear();

        // 遍历所有的学生数据
        for (Text val : values) {
            String[] parts = val.toString().split("\t");
            String examId = parts[0];
            String name = parts[1];
            int score = Integer.parseInt(parts[2]);

            // 存储相同分数的学生到 List 中
            top100.computeIfAbsent(score, k -> new ArrayList<>()).add(examId + "\t" + name);
        }

        // 输出前100名，输出总数达到100条时停止
        int count = 0;
        for (Integer score : top100.descendingKeySet()) {  // 从高分到低分遍历
            List<String> students = top100.get(score);

            // 输出该分数段的所有学生
            for (String student : students) {
                if (count >= 100) {
                    break;  // 输出达到100条则停止
                }
                context.write(key, new Text(student + "\t" + score));  // 输出科目、考号、姓名、分数
                count++;
            }

            // 如果已经输出满100条，直接跳出外层循环
            if (count >= 100) {
                break;
            }
        }
    }
}
