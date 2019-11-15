package mapreduce.writable;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver
{

    public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException
    {

        Path inputPath = new Path("C:\\Users\\rv00451128\\Desktop\\mylearning\\Project\\sparkcassandraexample\\word-count.txt");     // input file path
        Path outputDir = new Path("C:\\Users\\rv00451128\\Desktop\\mylearning\\data\\writable");      // output directory path

        Configuration conf = new Configuration();
        Job job = new Job(conf, "Word Count Writable");

        //name of driver class
        job.setJarByClass(WordCountDriver.class);
        //name of mapper class
        job.setMapperClass(WCMapper.class);
        //name of reducer class
        job.setReducerClass(WCReducer.class);

        job.setMapOutputKeyClass(WCWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(WCWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);

        outputDir.getFileSystem(job.getConfiguration()).delete(outputDir,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

        /*frank	7
        john	8
        kuoa	4
        lexa	1
        lupa	3
        smith	1
        steve	5*/
