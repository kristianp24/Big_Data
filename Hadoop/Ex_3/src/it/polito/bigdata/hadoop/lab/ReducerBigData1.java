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
                IntWritable,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
        int sumOfFreeApps = 0;
        int sumOfPaidApps = 0;
        for (IntWritable val : values){
            if (val.get() == 0){
                sumOfFreeApps++;
            }
            else{
                sumOfPaidApps++;
            }
            
           
        }

        if (sumOfFreeApps > sumOfPaidApps){
            context.write(key, new IntWritable(sumOfFreeApps));
        }
        

    	
    }
}
