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
 * [i-window, i+window] and increments the co-occurrence count of each
 * neighbor in t_i's local stripe.
 *
 * Window size and flush threshold are read at setup() from job conf:
 *   - pmi.window.size              (default 2)  k tokens on each side
 *   - pmi.stripes.flush.threshold  (default 50_000)  bounded in-mapper combining
 *
 * Input:  (byte_offset, line_text)
 * Output: (word, StripeWritable)  — emitted periodically and in cleanup()
 */


// Mapper types: <InputKey (byte offset), InputValue (line text), OutputKey (central word), OutputValue (stripe)>
public class StripesMapper extends Mapper<LongWritable, Text, Text, StripeWritable> {
    private int window = 2;
    private int flushThreshold = 50_000;
    private final HashMap<String, StripeWritable> stripes = new HashMap<>();
    private final Text outKey = new Text();

    @Override
    protected void setup(Context context) {
        window         = context.getConfiguration().getInt("pmi.window.size", 2);
        flushThreshold = context.getConfiguration().getInt("pmi.stripes.flush.threshold", 50_000);
    }

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
            int left  = Math.max(0, i - window);
            int right = Math.min(tokens.length - 1, i + window);

            for (int j = left; j <= right; j++) { // Iterate over neighbors
                if (j != i) {
                    stripe.increment(tokens[j]);
                }
            }
        }

        // Bounded in-mapper combining: spill when the HashMap grows past the
        // threshold to avoid OOM on large splits or wide windows.
        if (stripes.size() >= flushThreshold) flush(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        flush(context);
    }

    private void flush(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, StripeWritable> e : stripes.entrySet()) {
            outKey.set(e.getKey());
            context.write(outKey, e.getValue()); // emit partial stripe
        }
        stripes.clear();
    }
}