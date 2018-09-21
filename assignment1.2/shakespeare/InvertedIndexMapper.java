import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, InvertedIndex> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Get the name of the file
        final String work = ((FileSplit) context.getInputSplit()).getPath().getName();
        // Get the whole line
        final String line = value.toString();

        // Save the line number for all words on this line
        int lineNumber = -1;
        // For every word in this line
        for (final String word : line.split("\\W+")) {
            // If line number is null (first hit)
            if (lineNumber == -1) {
                // Check if it is the line number
                try {
                    lineNumber = Integer.parseInt(word);
                } catch (NumberFormatException nfe) {
                    // First word is not a line number!
                    System.out.println(line);
                    System.out.println(nfe.getMessage());
                    // Skip this whole line
                    break;
                }
            } else if (word.length() > 0) {
                // Write every word with its work and line number
                context.write(new Text(word.toLowerCase()), new InvertedIndex(work, lineNumber));
            }
        }
    }
}