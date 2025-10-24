package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    String prefix;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
       Configuration conf = context.getConfiguration();
       prefix = conf.get("filter.prefix", "");
    }
   
    @Override
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
            String[] words = value.toString().split("\\s+");
           
        if (prefix.isEmpty() == false){
                if (words[0].startsWith(prefix)){
                    context.write(new Text(words[0]), new IntWritable(Integer.parseInt(words[1])));
                }
        }
           
                
            
    }
}
