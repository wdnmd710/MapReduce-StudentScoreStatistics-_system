package StudentScore.ProvinceScore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * @Author: yuan
 */
public class ProvinceScoreDriver {
    public static void main(String[] args) throws Exception {
//        if (args.length != 2) {
//            System.err.println("Usage: ProvinceScoreSorter <input path> <output path>");
//            System.exit(-1);
//        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Province Score Sorter");
        job.setJarByClass(ProvinceScoreDriver.class);
        job.setMapperClass(ProvinceScoreMapper.class);
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setReducerClass(ProvinceScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("D:\\example\\input"));
       FileOutputFormat.setOutputPath(job, new Path("D:\\example\\output\\ProvinceScoreOutput"));

//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        MultipleOutputs.addNamedOutput(job, "province", FileOutputFormat.class, Text.class, Text.class);

        job.waitForCompletion(true);
    }

}
