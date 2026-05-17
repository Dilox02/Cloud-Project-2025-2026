package it.unipi.cloud.writables;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class StripeWritable implements Writable {

    private final HashMap<String, Integer> counts = new HashMap<>();

    /*
     * It's used by StripesMapper, it increments by 1 the co-occurance count of the
     * given neighbor word
     */
    public void increment(String word) {
        counts.merge(word, 1, Integer::sum);
    }

    /* It's used by StripesReducer, it merges another stripe into this */
    public void merge(StripeWritable other) {
        for (Map.Entry<String, Integer> e : other.counts.entrySet()) {
            counts.merge(e.getKey(), e.getValue(), Integer::sum);
        }
    }

    /* Returns the co-occurrence count for the given neighbor word or 0 */
    public int get(String word) {
        return counts.getOrDefault(word, 0);
    }

    /*
     * It's used by PMIMapper to obtain from the private count variable a set of
     * key-values pairs
     */
    public Set<Map.Entry<String, Integer>> entrySet() {
        return counts.entrySet();
    }

    // --- Writable binary serialization (used during shuffle) ---

    /*
     * Converts the object into bytes, it writes:
     * 1) Total number of pairs 2) For each pair: the word followed by its count
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(counts.size());
        for (Map.Entry<String, Integer> e : counts.entrySet()) {
            Text.writeString(out, e.getKey());
            out.writeInt(e.getValue());
        }
    }

    /*
     * By reading bytes received it rebuild each (word, count) pair in the exact
     * same order.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        counts.clear(); // it clean the buffer previously used
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            counts.put(Text.readString(in), in.readInt());
        }
    }

    // --- Text serialization (used by TextOutputFormat / TextInputFormat) ---

    /*
     * It's used by TextOutputFormat to write the output to HDFS, es
     * "word1:count1,word2:count2,...".
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Integer> e : counts.entrySet()) {
            if (sb.length() > 0)
                sb.append(",");
            sb.append(e.getKey()).append(":").append(e.getValue());
        }
        return sb.toString();
    }

    /*
     * it's Called by PMIMapper when reading Job 2's HDFS output as input for Job 3.
     */
    public static StripeWritable fromString(String s) {
        StripeWritable sw = new StripeWritable();
        if (s == null || s.trim().isEmpty())
            return sw;
        for (String pair : s.split(",")) {
            int colon = pair.lastIndexOf(':');
            if (colon > 0) {
                sw.counts.put(pair.substring(0, colon), Integer.parseInt(pair.substring(colon + 1)));
            }
        }
        return sw;
    }
}