package mapreduce.evenodd;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{


    public void map(LongWritable key, Text value, Context context)throws java.io.IOException, InterruptedException {

        String data[]=value.toString().split(",");    //data =  [85 131 993 392 689....]
        for(String num:data) {

            int number=Integer.parseInt(num);
            if((number%2)==1) {

                //give key as ODD and value as number. It means that our number is odd
                context.write(new Text("ODD"), new IntWritable(number));   //  ODD 85  131  993
            }
            else {
                //Give key as EVEN
                context.write(new Text("EVEN"), new IntWritable(number));   //  EVEN  392
            }

        }

    }
}
