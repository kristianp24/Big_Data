package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                IntWritable,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
        int counterLong = 0;
        int counterShort = 0;
        for (Text value : values){
            if (value.toString().equals("short"))
                counterShort++;
            else
                counterLong++;
        }
        
        String valueToReturn = counterShort + "," + counterLong;
        context.write(key, new Text(valueToReturn));
    }
   
}
