package it.unipi.cloud.job3;

import it.unipi.cloud.writables.StripeWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** 
 * Job 3 — Mapper (PMI computation via DistributedCache join).
 *
 * setup():  reads word_counts.tsv from DistributedCache into a local
 *           HashMap and extracts N from the "__TOTAL__" entry.
 *
 * map():    for each (word_x, stripe) from Job 2, iterates over every
 *           neighbor word_y and computes:
 *             PMI(x,y) = log2( c(x,y) * N / ( c(x) * c(y) ) )
 *
 * Input:  (byte_offset, "word_x\tstripe_text")   — Job 2 output
 * Output: ("word_x\tword_y", pmi_value)
 * 
 * for clarity \t is the separator used in tsv to divide columns :) 
 */
// Mapper types: <InputKey (byte offset), InputValue (word_x + tab + stripe), OutputKey (pair word_x\tword_y), OutputValue (PMI score)>
public class PMIMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    
    private final HashMap<String, Long> wordFreq = new HashMap<>(); // used store individual word frequencies c(x) loaded from Job 1
    private long N = 0; // Global token count (N) extracted from the special "__TOTAL__" record

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        // Hadoop's DistributedCache copies 'word_counts.tsv' from HDFS to the local disk of this machine,
        // creating a shortcut (symlink) in the current folder. This allows us to read it directly by name.
        try (BufferedReader br = new BufferedReader(new FileReader("word_counts.tsv"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length != 2) continue; // Skip malformed lines
                
                long count = Long.parseLong(parts[1].trim());
                
                // Intercept the special token to set the global N, otherwise store the word frequency
                if (parts[0].equals("__TOTAL__")) {
                    N = count;
                } else {
                    wordFreq.put(parts[0].trim(), count);
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String line = value.toString();
        int tab = line.indexOf('\t');
        if (tab < 0) return; // sanity-check: skip lines that don't contain a tab separator

        String wordX = line.substring(0, tab).trim();
        String stripeText = line.substring(tab + 1).trim();

        // Retrieve the unigram count c(x) for the central word from our in-memory cache
        Long cx = wordFreq.get(wordX);
        if (cx == null || cx == 0 || N == 0) return; // Safety check to avoid Division by Zero

        // Deserializes the text back into a structured StripeWritable object
        StripeWritable stripe = StripeWritable.fromString(stripeText);
        
        // Reusable output key and value objects to optimize memory management
        Text outKey = new Text();
        DoubleWritable outVal = new DoubleWritable();

        // Iterate through each neighbor (wordY) present in wordX's co-occurrence stripe
        for (Map.Entry<String, Integer> e : stripe.entrySet()) {
            String wordY = e.getKey();
            long cxy = e.getValue(); // Co-occurrence count c(x,y)
            
            // Retrieve the unigram count c(y) for the neighbor word
            Long cy = wordFreq.get(wordY);
            if (cy == null || cy == 0) continue; // Skip if frequency data is missing

            // Formula: PMI(x,y) = log2( (c(x,y) * N) / (c(x) * c(y)) )
            // Java's Math.log is natural log (ln), so we divide by ln(2) to change base to log2
            double pmi = Math.log((double) cxy * N / ((double) cx * cy)) / Math.log(2);
            
            outKey.set(wordX + "\t" + wordY);
            outVal.set(pmi);
            context.write(outKey, outVal); // Emit the pair and its computed PMI score
        }
    }
}