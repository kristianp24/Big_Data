package it.polito.bigdata.hadoop2;

import java.io.IOException;

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
    
   
    @Override
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
            String[] words = value.toString().split("\\s+");
            if (Integer.parseInt(words[1]) >= 0 && Integer.parseInt(words[1]) < 100){
                context.write(new Text("Group0"), new IntWritable(1));
            } 
            else if (Integer.parseInt(words[1]) >= 100 && Integer.parseInt(words[1]) < 200) {
                context.write(new Text("Group1"), new IntWritable(1));
            }
            else if(Integer.parseInt(words[1]) >= 200 && Integer.parseInt(words[1]) < 300){
                context.write(new Text("Group2"), new IntWritable(1));
            }
            else if(Integer.parseInt(words[1]) >= 300 && Integer.parseInt(words[1]) < 400){
                context.write(new Text("Group3"), new IntWritable(1));
            }
            else if(Integer.parseInt(words[1]) >= 400 && Integer.parseInt(words[1]) < 500){
                context.write(new Text("Group4"), new IntWritable(1));
            }
            else{
                context.write(new Text("Group5"), new IntWritable(1));
            }
                
            
    }
}
