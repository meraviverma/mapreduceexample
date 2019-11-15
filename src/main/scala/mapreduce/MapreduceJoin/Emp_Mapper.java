package mapreduce.MapreduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Emp_Mapper extends Mapper<LongWritable,Text,Text,Text> {
    //1,Jack

    public void mapper(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
        String line=value.toString();

        String[] EmployeeInfo=line.split(","); //EmployeeInfo= [{1} {jack}]

        context.write(new Text(EmployeeInfo[0]), new Text("Emp,"+EmployeeInfo[1]));
        //1     Emp,Jack
    }
}
