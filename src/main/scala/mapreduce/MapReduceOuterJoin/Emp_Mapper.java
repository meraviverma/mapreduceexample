package mapreduce.MapReduceOuterJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Emp_Mapper extends Mapper<LongWritable,Text,Text,Text> {
    public void map(LongWritable key,Text V,Context con) throws IOException,InterruptedException{

        String line=V.toString().trim();    //convert incoming record to string

        String[] EmployeeData=line.split(","); //[{1281} {Shawn} {Architect} {7890} {1481} {10} ]

        con.write(new Text(EmployeeData[5]), new Text("Employee,"+EmployeeData[0]+" "+EmployeeData[1]+" "+EmployeeData[2]+" "+EmployeeData[3]+" "+EmployeeData[4]));
    }
}
