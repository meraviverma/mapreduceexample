package mapreduce.ChainingMapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FirstMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    private static final IntWritable ONE=new IntWritable(1);

    protected void map(LongWritable key,Text value,Context c)throws IOException,java.lang.InterruptedException{
        String[] words = value.toString().split(",");

        for(String word:words){
            c.write(new Text(word),ONE);
        }
    }
}
