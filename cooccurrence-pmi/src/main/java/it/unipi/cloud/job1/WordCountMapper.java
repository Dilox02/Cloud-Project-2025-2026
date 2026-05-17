package it.unipi.cloud.job1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Job 1 — Mapper with in-mapper combining.
 * Accumulates word counts in a local HashMap during map() and flushes
 * them all at once in cleanup(). This avoids emitting (word, 1) for
 * every token occurrence, dramatically reducing shuffle traffic.
 * 
 * Input:  (byte_offset, line_text)
 * Output: (word, local_aggregate_count)
 */

// Mapper types: <InputKey (byte offset), InputValue (line text), OutputKey (word), OutputValue (word count)>
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final HashMap<String, Long> localCounts = new HashMap<>();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // Convert to lowercase and remove numbers, punctuation, and non-alpha
        // characters
        String line = value.toString().toLowerCase().replaceAll("[^a-z\\s]", " "); // Regex, replace all different from
                                                                                   // the argument
        for (String word : line.split("\\s+")) { // Regex, 1 or more white spaces
            if (!word.isEmpty()) {
                localCounts.merge(word, 1L, Long::sum);
            }
        }
    }

    // It emits aggregated key-values pairs for the next phase
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Text outKey = new Text();
        LongWritable outVal = new LongWritable();
        for (Map.Entry<String, Long> e : localCounts.entrySet()) {
            outKey.set(e.getKey());
            outVal.set(e.getValue());
            context.write(outKey, outVal);
        }
    }
}
