package it.unipi.cloud.job3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Job 3 — Reducer.
 *
 * Each (word_x, word_y) pair normally comes from a single mapper, so
 * this is essentially a pass-through. Multiple values are averaged in
 * case of duplicates. Only PPMI >= 0 pairs are emitted.
 *
 * The output lines are written by TextOutputFormat as:
 *   word_x \t word_y \t pmi_value
 * (the key itself already contains "word_x\tword_y")
 *
 * Input:  ("word_x\tword_y", [pmi_values])
 * Output: ("word_x\tword_y", pmi)   — only if pmi >= 0 (PPMI)
 */
// Reducer types: <InputKey (pair word_x\tword_y), InputValue (list of partial PMI scores), OutputKey (pair), OutputValue (final PPMI score)>
public class PMIReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private final DoubleWritable outVal = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        
        double sum = 0;
        int count = 0;
        
        // Loop through all PMI values received for the current "word_x\tword_y" key
        for (DoubleWritable v : values) {
            sum += v.get(); // Accumulate the scores
            count++;        // Count how many occurrences/duplicates were found
        }
        
        // Compute the mathematical average (handles rare duplicate cases safely)
        double pmi = sum / count;
        
        // PPMI Filter (Positive Pointwise Mutual Information):
        // In linguistics, a negative PMI means the words co-occur less than expected by pure chance.
        // We drop negative correlations and keep only scores >= 0.
        if (pmi >= 0) {
            outVal.set(pmi);             // Set the final score into the Writable container
            context.write(key, outVal);  // Emit to HDFS ("word_x\tword_y \t pmi_value")
        }
    }
}