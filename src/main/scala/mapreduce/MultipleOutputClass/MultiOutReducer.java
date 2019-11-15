package mapreduce.MultipleOutputClass;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Iterator;

// DJPX255251   [{Arthur,HR,6397} {Arthur,HR,6597} {Arthur,HR,6797}]
public class MultiOutReducer extends Reducer<Text,Text,Text,Text> {

    //creating object of multiple output class
    private MultipleOutputs<Text,Text> out;

    //Initializing the out object by passing the context object as argument
    //runs only one time for a job
    @Override
    protected void setup(Context c) throws IOException,InterruptedException {
        out = new MultipleOutputs(c);
    }

    //It has to do two task. First calculate salary of employee for three year and then check from which department that employee is.
    //If it is from HR then output that keyvalue pair in HR file and if it is accounts employee output that in Accounts file
    @Override
    protected  void reduce(Text key,Iterable<Text> values,Context c) throws IOException,InterruptedException{

        int totalSalary = 0;
        String dept = "";
        String name = "";

        Iterator<Text> valueIter = values.iterator();

        while(valueIter.hasNext()){
            /* name,department,salary */
            String[] data = valueIter.next().toString().split(","); //[{ Arthur} {HR} {6397}]
            name = data[0];  //  name = Arthur
            dept = data[1]; //  dept = HR
            totalSalary += Integer.parseInt(data[2]); //  totalSalary  = 19791
        }

        /* output employee salary to department file */
        if(dept.equalsIgnoreCase("hr")){
            out.write("HR",key,new Text(name + "," + totalSalary)); //
        }else if(dept.equalsIgnoreCase("accounts")){
            out.write("Accounts",key,new Text(name + "," + totalSalary));
        }
    }

    @Override
    protected void cleanup(Context c) throws IOException,InterruptedException{
        out.close();
    }
}
