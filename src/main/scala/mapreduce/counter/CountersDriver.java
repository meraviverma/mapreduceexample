package mapreduce.counter;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountersDriver
{

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {

        Path inputPath = new Path("C:\\Users\\rv00451128\\Desktop\\mylearning\\Project\\sparkcassandraexample\\counter.txt");
        Path outputDir = new Path("C:\\Users\\rv00451128\\Desktop\\mylearning\\data\\counter");

        Configuration conf = new Configuration();
        Job job = new Job(conf, "Counters");

        // name of driver class
        job.setJarByClass(CountersDriver.class);
        //name of Mapper class
        job.setMapperClass(CounterMapper.class);
        // name of Reducer class
        job.setReducerClass(CounterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);
        outputDir.getFileSystem(job.getConfiguration()).delete(outputDir,true);
        job.waitForCompletion(true);



    }
}

