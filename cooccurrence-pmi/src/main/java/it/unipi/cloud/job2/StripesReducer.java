package it.unipi.cloud.job2;

import it.unipi.cloud.writables.StripeWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Job 2 — Reducer.
 *
 * Merges all partial stripes for the same word by summing co-occurrence
 * counts for common neighbors.
 *
 * Input:  (word, [stripe_1, stripe_2, ...])
 * Output: (word, merged_stripe)
 */

// Reducer types: <InputKey (central word), InputValue (partial stripes), OutputKey (central word), OutputValue (fully merged stripe)>
public class StripesReducer extends Reducer<Text, StripeWritable, Text, StripeWritable> {

    @Override
    protected void reduce(Text key, Iterable<StripeWritable> values, Context context)
            throws IOException, InterruptedException {
        
        StripeWritable merged = new StripeWritable(); 
        
        // Iterate through all partial stripes received from the Mappers for this central word
        for (StripeWritable stripe : values) {
            merged.merge(stripe);
        }
        
        context.write(key, merged);
    }
}