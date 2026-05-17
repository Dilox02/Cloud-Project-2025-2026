package it.unipi.cloud;

import it.unipi.cloud.job1.WordCountMapper;
import it.unipi.cloud.job1.WordCountReducer;
import it.unipi.cloud.job2.StripesMapper;
import it.unipi.cloud.job2.StripesReducer;
import it.unipi.cloud.job3.PMIMapper;
import it.unipi.cloud.job3.PMIReducer;
import it.unipi.cloud.writables.StripeWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * Entry point — orchestrates the three MapReduce jobs in sequence:
 *
 *   Job 1  raw text  →  word_counts/        (word frequencies + N)
 *   Job 2  raw text  →  co_occurrence/      (stripes per word)
 *   Job 3  stripes   →  pmi_output/         (PPMI pairs)
 *
 * Usage:
 *   hadoop jar cooccurrence-pmi.jar it.unipi.cloud.Driver <input> <output_base>
 */
public class Driver {

    public static void main(String[] args) throws Exception {
        // Validate command-line arguments
        if (args.length != 2) {
            System.err.println("Usage: Driver <input_path> <output_base_dir>");
            System.exit(1);
        }

        // Define HDFS paths for input and intermediate/final outputs
        String inputPath    = args[0];
        String outputBase   = args[1];
        String wordCountsOut = outputBase + "/word_counts";
        String stripesOut    = outputBase + "/co_occurrence";
        String pmiOut        = outputBase + "/pmi_output";

        // Initialize Hadoop configuration
        Configuration conf = new Configuration();

        // ----------------------------------------------------------------
        // Job 1 — Word Count + Corpus Statistics
        // ----------------------------------------------------------------
        Job job1 = Job.getInstance(conf, "Word Count");
        job1.setJarByClass(Driver.class);
        
        // Set Mapper and Reducer classes for Job 1
        job1.setMapperClass(WordCountMapper.class);
        job1.setReducerClass(WordCountReducer.class);
        
        // CRUCIAL: Force 1 reducer to guarantee a single output file (part-r-00000)
        // for DistributedCache and ensure a correct global __TOTAL__ count.
        job1.setNumReduceTasks(1);
        
        // Configure Map output key/value types
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);
        
        // Configure Final output key/value types
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        
        // Set input and output paths for Job 1
        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(wordCountsOut));
        
        // Block execution until Job 1 finishes; exit if it fails
        if (!job1.waitForCompletion(true)) System.exit(1);

        // ----------------------------------------------------------------
        // Job 2 — Co-occurrence with Stripes
        // ----------------------------------------------------------------
        Job job2 = Job.getInstance(conf, "Co-occurrence Stripes");
        job2.setJarByClass(Driver.class);
        
        // Set Mapper and Reducer classes for Job 2
        job2.setMapperClass(StripesMapper.class);
        job2.setReducerClass(StripesReducer.class);
        
        // Configure Map and Final output types using the custom StripeWritable map structure
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(StripeWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(StripeWritable.class);
        
        // Set input and output paths for Job 2 (reads from the same raw text)
        FileInputFormat.addInputPath(job2, new Path(inputPath));
        FileOutputFormat.setOutputPath(job2, new Path(stripesOut));
        
        // Block execution until Job 2 finishes; exit if it fails
        if (!job2.waitForCompletion(true)) System.exit(1);

        // ----------------------------------------------------------------
        // Job 3 — PMI Computation
        // ----------------------------------------------------------------
        Job job3 = Job.getInstance(conf, "PMI Computation");
        job3.setJarByClass(Driver.class);
        
        // Set Mapper and Reducer classes for Job 3
        job3.setMapperClass(PMIMapper.class);
        job3.setReducerClass(PMIReducer.class);
        
        // Configure Map and Final output types (Emits pairs and their double PMI score)
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(DoubleWritable.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);

        // DistributedCache Configuration:
        // Resolve the full HDFS URI for the Job 1 output file ('part-r-00000').
        // The '#' symlink syntax automatically links the file into the task's 
        // local working directory as 'word_counts.tsv' for map-side join.
        Path wcFile = new Path(wordCountsOut + "/part-r-00000");
        FileSystem fs = wcFile.getFileSystem(conf);
        URI wcUri = new URI(fs.resolvePath(wcFile).toString() + "#word_counts.tsv");
        job3.addCacheFile(wcUri);

        // Input-Output Pipeline Setup:
        // Job 3 takes the co-occurrence output from Job 2 as its main input data stream
        FileInputFormat.addInputPath(job3, new Path(stripesOut));
        FileOutputFormat.setOutputPath(job3, new Path(pmiOut));
        
        // Block execution until Job 3 finishes; exit if it fails
        if (!job3.waitForCompletion(true)) System.exit(1);

        // Signal successful end of the pipeline
        System.out.println("Pipeline completed. Output: " + pmiOut);
    }
}