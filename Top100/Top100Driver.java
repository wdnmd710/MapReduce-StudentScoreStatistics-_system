package StudentScore.Top100;

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
public class Top100Driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 100 per Subject");
        job.setJarByClass(Top100Driver.class);

        // 设置 Mapper 和 Reducer 类
        job.setMapperClass(Top100Mapper.class);
        job.setReducerClass(Top100Reducer.class);

        // 设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path("D:\\example\\input"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\example\\output\\top100"));
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交作业
         job.waitForCompletion(true);
    }
}
