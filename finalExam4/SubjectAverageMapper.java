package StudentScore.finalExam4;

/**
 * @Bear
 **/
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SubjectAverageMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text subject = new Text();
    private IntWritable score = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 跳过标题行
        if (value.toString().startsWith("考号")) {
            return;
        }

        // 按制表符分割输入行
        String[] fields = value.toString().split("\t");
        if (fields.length < 9) {
            return; // 确保数据完整性
        }

        // 提取成绩
        int chinese = Integer.parseInt(fields[2]);
        int math = Integer.parseInt(fields[3]);
        int english = Integer.parseInt(fields[4]);
        int politics = Integer.parseInt(fields[5]);
        int biology = Integer.parseInt(fields[6]);
        int physics = Integer.parseInt(fields[7]);

        // 输出科目和对应的成绩
        subject.set("语文");
        score.set(chinese);
        context.write(subject, score);

        subject.set("数学");
        score.set(math);
        context.write(subject, score);

        subject.set("英语");
        score.set(english);
        context.write(subject, score);

        subject.set("政治");
        score.set(politics);
        context.write(subject, score);

        subject.set("生物");
        score.set(biology);
        context.write(subject, score);

        subject.set("物理");
        score.set(physics);
        context.write(subject, score);
    }
}