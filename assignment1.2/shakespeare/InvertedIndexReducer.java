import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class InvertedIndexReducer extends Reducer<Text, InvertedIndex, Text, Text> {

    public void reduce(Text key, Iterable<InvertedIndex> values, Context context) throws IOException, InterruptedException {
        final StringBuilder builder = new StringBuilder();
        // For every inverted index of the same word
        for (final InvertedIndex val : values) {
            // Append it as 'work@line'
            builder.append(val.toString());
            // Not the last one?
            if (values.iterator().hasNext()) {
                // Separate with commas
                builder.append(", ");
            }
        }
        // Write the word with all its indices as a single line
        context.write(key, new Text(builder.toString()));
    }
}