package StudentScore.StudentTotalScore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: yuan
 */
public class TotalScoreDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Total Score Descending Sorter");
        job.setJarByClass(TotalScoreDriver.class);

        // 设置 Mapper 和 Reducer 类
        job.setMapperClass(TotalScoreMapper.class);
        job.setReducerClass(TotalScoreReducer.class);

        // 设置输出类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path("D:\\example\\input"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\example\\output\\global-output"));

//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置 Reduce 任务数量为 1，确保全局排序
        job.setNumReduceTasks(1);

        // 提交作业
       job.waitForCompletion(true);
    }
}
