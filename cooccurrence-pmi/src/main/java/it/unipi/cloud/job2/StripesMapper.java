package it.unipi.cloud.job2;

import it.unipi.cloud.writables.StripeWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Job 2 — Mapper with in-mapper combining for Stripes.
 *
 * For every token t_i in a line, looks at the sliding window
 * [i-WINDOW, i+WINDOW] and increments the co-occurrence count of each
 * neighbor in t_i's local stripe. 
 * 
 * Window size k=2 means 2 tokens to the left + 2 to the right.
 *
 * Input:  (byte_offset, line_text)
 * Output: (word, StripeWritable)  — emitted in cleanup()
 */


// Mapper types: <InputKey (byte offset), InputValue (line text), OutputKey (central word), OutputValue (associative array/stripe)>
public class StripesMapper extends Mapper<LongWritable, Text, Text, StripeWritable> {
    private static final int WINDOW = 2;
    private final HashMap<String, StripeWritable> stripes = new HashMap<>();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String[] tokens = value.toString()
                .toLowerCase()
                .replaceAll("[^a-z\\s]", " ")
                .trim()
                .split("\\s+");

        // sanity check: Skip blank or empty lines ( trim().split() on empty string gives an array with a single empty string)
        if (tokens.length == 1 && tokens[0].isEmpty()) return;

        for (int i = 0; i < tokens.length; i++) {
            String center = tokens[i];
            stripes.putIfAbsent(center, new StripeWritable()); // Initialize a new StripeWritable
            StripeWritable stripe = stripes.get(center);

            // Compute boundaries for the sliding window
            int left  = Math.max(0, i - WINDOW);
            int right = Math.min(tokens.length - 1, i + WINDOW);
            
            for (int j = left; j <= right; j++) { // Iterate over neighbors
                if (j != i) {
                    stripe.increment(tokens[j]);
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Text outKey = new Text();
        
        for (Map.Entry<String, StripeWritable> e : stripes.entrySet()) {
            outKey.set(e.getKey());
            context.write(outKey, e.getValue()); // Emit (central_word, accumulated_stripe)
        }
    }
}