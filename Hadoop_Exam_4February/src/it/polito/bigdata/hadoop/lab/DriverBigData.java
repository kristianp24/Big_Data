package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Import the single Mapper and Reducer
import it.polito.bigdata.hadoop.lab.MapperBigData1;
import it.polito.bigdata.hadoop.lab.ReducerBigData1;


/**
 * MapReduce program (Single Job Solution)
 */
public class DriverBigData extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        int exitCode;

        Configuration conf = this.getConf();

        // -----------------------------------------------------
        // JOB 1: Tally user types and filter in a single step
        // -----------------------------------------------------
        Job job = Job.getInstance(conf);

        // Assign a name to the job
        job.setJobName("PoliMeeting - 100% Free Countries (Single Job)");

        Path inputPath;
        Path outputDir;
        int numberOfReducers;

        // Parse the parameters
        numberOfReducers = Integer.parseInt(args[0]);
        inputPath = new Path(args[1]);
        outputDir = new Path(args[2]); // This is the final output directory

        // Set the path of the input file/folder for this job (Users.txt)
        FileInputFormat.addInputPath(job, inputPath);

        // Set the path of the output folder for this job
        FileOutputFormat.setOutputPath(job, outputDir);

        // Specify the class of the Driver for this job
        job.setJarByClass(DriverBigData.class);

        // Set job input format
        job.setInputFormatClass(TextInputFormat.class);

        // Set job output format
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set map class
        job.setMapperClass(MapperBigData1.class);

        // Set map output key and value classes
        // Key: Country (Text)
        // Value: 1 if "free", 0 if "not free" (IntWritable)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Set reduce class
        job.setReducerClass(ReducerBigData1.class);

        // Set reduce output key and value classes
        // Key: Country (Text)
        // Value: Total user count (if all free)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set number of reducers
        job.setNumReduceTasks(numberOfReducers);

        // Execute the job and wait for completion
        if (job.waitForCompletion(true) == true) {
            exitCode = 0;
        } else {
            exitCode = 1;
        }

        return exitCode;
    }

    /**
     * Main of the driver
     */

    public static void main(String args[]) throws Exception {
        // Exploit the ToolRunner class to "configure" and run the Hadoop
        // application
        int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);

        System.exit(res);
    }
}

