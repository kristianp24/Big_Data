package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

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
                Text> { 
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
        
        int[] scoruri = {0, 0, 0, 0, 0, 0, 0};
        for (Text value: values){
            if (value.toString().equals("A"))
                scoruri[0]++;
            else if (value.toString().equals("B"))
                scoruri[1]++;
            else if (value.toString().equals("C"))
                scoruri[2]++;
            else if (value.toString().equals("D"))
                scoruri[3]++;
            else if (value.toString().equals("E"))
                scoruri[4]++;
            else if (value.toString().equals("F"))
                scoruri[5]++;
            else
                scoruri[6]++;
        }


        int pozitieValMax = findMax(scoruri);
        if (pozitieValMax == 0)
            context.write(key, new Text("A"));
        else if (pozitieValMax == 1)
            context.write(key, new Text("B"));
        else if (pozitieValMax == 2)
            context.write(key, new Text("C"));
        else if (pozitieValMax == 3)
            context.write(key, new Text("D"));
        else if (pozitieValMax == 4)
            context.write(key, new Text("E"));
        else if (pozitieValMax == 5)
            context.write(key, new Text("F"));
        else
            context.write(key, new Text("G"));       
        
    }

    private int findMax(int[] scoruri){
        int max = 0;
        int maxIndex = 0;
        for(int i = 0; i<7; i++){
            if(scoruri[i] > max){
                
                maxIndex = i;
                 max = scoruri[i];
            }
           
                
        }  
        return maxIndex;
    }

}
