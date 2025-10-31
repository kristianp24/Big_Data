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
                IntWritable> {  // Output value type
    
    TopKVector<WordCountWritable> top100_global;
    
    protected void setup(Context context) throws IOException, InterruptedException {
        top100_global=new TopKVector<>(100);
    }

    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
        //top 100 global
        int suma=0;
        
       // WordCountWritable cuplu= new WordCountWritable(key.toString() , values[0])

        for(IntWritable value:values){
            suma+= value.get();
        }

        WordCountWritable cuplu=new WordCountWritable(key.toString(),suma);

        top100_global.updateWithNewElement(cuplu);
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(WordCountWritable value:top100_global.getLocalTopK()){
            context.write(new Text(value.getWord()), new IntWritable(value.getCount()));
        }

   }
}
