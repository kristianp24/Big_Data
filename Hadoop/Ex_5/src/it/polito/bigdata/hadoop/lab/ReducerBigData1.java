package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<javax.xml.soap.Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

		int count = 0;
        int aux = 1;
        
        for (Text val : values){
           if (val.toString().equals("free")){
            count++;
           }
           else{
            aux = -1;
            break;
           }
        }
        
        if (aux == -1)
            return;
        
        context.write(key, new IntWritable(count));
    	
    }
}
