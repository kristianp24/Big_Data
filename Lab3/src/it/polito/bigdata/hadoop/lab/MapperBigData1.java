package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
        
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
            String[] line = context.toString().split(",");
            for (int i = 0; i < line.length; i++){
                if (line[i].startsWith("B")){
                    if (i == line.length - 1){
                    break;
                    }
                    String word1 = line[i];
                    String word2 = line[i+1];
                    String pair = word1 + "," + word2;
                    context.write(new Text(pair), new IntWritable(1));
                }
                
            } 
            
    }
}
