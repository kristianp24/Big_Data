package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

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
                    Text> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        String[] lines = value.toString().split(",");
        if (lines[0].equals("BuildingID"))
                return;
        
        String keyOutput = lines[1] + "," + lines[5];
        String energyEfficiency = lines[6];

        context.write(new Text(keyOutput), new Text(energyEfficiency));
    }
}
