package StudentScore.ProvinceScore;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author: yuan
 */
public class ProvincePartitioner extends Partitioner<Text,Text> {
    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        String province = key.toString();
        return (province.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
