package mapreduce.facebook_ads;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class FacebookDriver {

        public static void main(String[] args) throws IOException,	ClassNotFoundException, InterruptedException
        {

            Path input_path = new Path("C:\\Users\\rv00451128\\Desktop\\mylearning\\Project\\sparkcassandraexample\\fb.txt");
            Path output_dir = new Path("C:\\Users\\rv00451128\\Desktop\\mylearning\\data\\fb");

            Configuration conf = new Configuration();
            Job job = new Job(conf, "Facebook analysis");

            // name of driver class
            job.setJarByClass(FacebookDriver.class);
            // name of mapper class
            job.setMapperClass(FacebookMapper.class);
            // name of reducer class
            job.setReducerClass(FacebookReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, input_path);
            FileOutputFormat.setOutputPath(job, output_dir);
            output_dir.getFileSystem(job.getConfiguration()).delete(output_dir,	true);

            job.waitForCompletion(true);
        }
}

//Output//
/*{Delhi=69.71352921559776,7, Mumbai=65.11269276393831,3, Hyderabad=14.962096615810141,4, Bangalore=9.794102083032664,3}
        {Delhi=11.35870884525211,3, Hyderabad=2.0242914979757085,1}
        {Delhi=23.789586233364346,4, Mumbai=1.834862385321101,1, Hyderabad=14.80364025093538,3, Bangalore=38.34708677169293,3}
        {Delhi=18.333951522668016,6, Mumbai=5.485519198163804,2, Hyderabad=63.38267305260219,10, Bangalore=1.8306636155606408,1}*/

