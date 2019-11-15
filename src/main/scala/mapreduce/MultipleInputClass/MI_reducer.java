package mapreduce.MultipleInputClass;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MI_reducer  extends Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context con)throws IOException, InterruptedException
    {
        int sum = 0;
        for (IntWritable count : values)
        {
            sum = sum + count.get();
        }
        con.write(key, new IntWritable(sum));

    }

}
