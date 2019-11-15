package mapreduce.MultipleOutputClass;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*I want to calculate total salary of employee for all his three years
calculated result should go in different files. HR should go in one file and*/

//DJPX255251,Arthur,HR,6397,2016
public class MultiOutMapper extends Mapper<LongWritable,Text,Text,Text> {

    private Text empId=new Text();
    private Text empData=new Text();

    @Override
    public void map(LongWritable key,Text value,Context c) throws IOException,InterruptedException{

        String line = value.toString();
        String[] words = line.split(",");

        empId.set(words[0]);
        empData.set(words[1]+ "," + words[2] + "," + words[3]);

        c.write(empId,empData);
        //DJPX255251 Arthur,HR,6397
        //Key           value

    }
}
