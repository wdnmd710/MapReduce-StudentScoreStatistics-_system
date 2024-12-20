package StudentScore.ProvinceScore;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @Author: yuan
 */
public class ProvinceScoreReducer extends Reducer<Text,Text,Text,Text> {
    private MultipleOutputs<Text, Text> multipleOutputs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> students = new LinkedList<>();
        for (Text val : values) {
            students.add(val.toString());
        }

        // Sort students by total score in descending order
        Collections.sort(students, (a, b) -> {
            int scoreA = Integer.parseInt(a.split(",")[0]);
            int scoreB = Integer.parseInt(b.split(",")[0]);
            return Integer.compare(scoreB, scoreA);
        });

        for (String student : students) {
            String[] parts = student.split(",", 2);
            multipleOutputs.write(new Text(parts[1]), null, key.toString() + "/part");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
