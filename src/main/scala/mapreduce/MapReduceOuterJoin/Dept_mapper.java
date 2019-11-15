package mapreduce.MapReduceOuterJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Dept_mapper extends Mapper<LongWritable,Text,Text,Text> {

    public void map(LongWritable key,Text V1,Context context) throws IOException,InterruptedException{
        String line=V1.toString().trim();

        String[] Temparray=line.split(",");

        context.write(new Text(Temparray[0]), new Text("department,"+Temparray[1]+" "+Temparray[2]));
    }
}
