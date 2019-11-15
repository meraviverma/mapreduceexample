package mapreduce.MapReduceOuterJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OuterJoin_Driver {
    public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException{

        Path employeeFilePath = new Path("C:\\Users\\rv00451128\\Desktop\\mylearning\\Project\\sparkcassandraexample\\employee");
        Path deptFilePath=new Path("C:\\Users\\rv00451128\\Desktop\\mylearning\\Project\\sparkcassandraexample\\dept.txt");

        Path OutputDirectory = new Path("C:\\Users\\rv00451128\\Desktop\\mylearning\\data\\mapreduceouterjoin");

        Configuration C1=new Configuration();

        Job job=new Job(C1,"OuterJoin");

        //name of driver class
        job.setJarByClass(OuterJoin_Driver.class);

        //name of Mapper1 class
        job.setMapperClass(Emp_Mapper.class);

        //name of Mapper2 class
        job.setMapperClass(Dept_mapper.class);

        //name of reducer class

        job.setReducerClass(OuterJoin_reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job,employeeFilePath,TextInputFormat.class,Emp_Mapper.class); //employee file will processed by Emp_Mapper
        MultipleInputs.addInputPath(job,deptFilePath,TextInputFormat.class,Dept_mapper.class); //dept file will processed by Dep_Mapper

        FileOutputFormat.setOutputPath(job,OutputDirectory);
        OutputDirectory.getFileSystem(job.getConfiguration()).delete(OutputDirectory,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
