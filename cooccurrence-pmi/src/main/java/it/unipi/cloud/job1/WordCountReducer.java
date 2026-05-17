package it.unipi.cloud.job1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Job 1 — Reducer.
 * Sums partial counts for each word. Tracks a running total N and
 * emits the special key "__TOTAL__" in cleanup() so Job 3 can read N
 * from the same file without a separate counter.
 *
 * NOTE: Job 1 must use exactly 1 reducer so "__TOTAL__" is correct
 *
 * Input: (word, [partial_counts...])
 * Output: (word, total_count) + (__TOTAL__, N)
 */

// Reducer types: <InputKey (word), InputValue (partial counts), OutputKey (word), OutputValue (final count)>
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private static final Text TOTAL_KEY = new Text("__TOTAL__"); // Global key used for counting the total number of
                                                                 // words
    private final LongWritable outVal = new LongWritable();
    private long totalTokens = 0L; // Global counter to accumulate the total number of words

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0;

        // Aggregate all partial counts received from the Mappers for the current word
        for (LongWritable v : values) {
            sum += v.get();
        }

        totalTokens += sum;
        outVal.set(sum);
        context.write(key, outVal); // Emit the final count for the current word
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        outVal.set(totalTokens);
        context.write(TOTAL_KEY, outVal);
    }
}