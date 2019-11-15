package mapreduce.MapReduceOuterJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OuterJoin_reducer extends Reducer<Text,Text,Text,Text> {

    // 	  20     [ {Employee,1381 Jacob Admin 4560 1481}  {dept,ACCOUNTS  PUNE} ]
    public void reduce(Text DeptNumber,Iterable<Text> Values,Context context) throws IOException,InterruptedException{

        //creating list
        List<String> Employee_List = new ArrayList<String>();
        String Department="";

        //creating object of iterator class
        //Iterator through a particular key
        Iterator<Text> Itr=Values.iterator(); // declaring iterator for incoming list of values

        while (Itr.hasNext()){

            Text data=Itr.next();   //data = Employee,1381 Jacob Admin 4560 1481

            String NewRecord[] = data.toString().split(",");    // NewRecord =  [ {Employee} {1381 Jacob Admin 4560 1481} ]

            if(NewRecord[0].equalsIgnoreCase("Employee")){
                Employee_List.add(NewRecord[1]);
            }else if(NewRecord[0].equalsIgnoreCase(("department"))){
                Department = NewRecord[1];
            }
        }

        //For joining our both list should be non empty
        if(!Employee_List.isEmpty() && !Department.isEmpty()){  //Condition for Inner Join
            for(String E : Employee_List){
                context.write(DeptNumber,new Text(E + " "+Department)); //output
            }
        }

        if(!Employee_List.isEmpty() && Department.isEmpty()){ //condition for left outer join
            for(String E : Employee_List){
                context.write(DeptNumber, new Text(E + " " + "null_value null_value")); //output
            }
        }

        //Condition for Right Outer join

        if (Employee_List.isEmpty() && !Department.isEmpty())
        {
            context.write(DeptNumber, new Text("null_value null_value null_value null_value null_value" + " " + Department));    //output
        }
    }
}
