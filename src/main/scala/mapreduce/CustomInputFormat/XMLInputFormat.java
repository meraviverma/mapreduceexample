package mapreduce.CustomInputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

//create our own custom input format class
public class XMLInputFormat extends TextInputFormat {
    //RecordReader is the actual file that convert input file to key value pair and then provide it to my input format class
    //
    //The record reader breaks the data into key/value pairs for input to the mapper
    @Override
    public RecordReader<LongWritable,Text> createRecordReader(InputSplit split, TaskAttemptContext context){
        return new XMLRecordReader();
    }

}
