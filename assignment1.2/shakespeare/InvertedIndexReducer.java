import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class InvertedIndexReducer extends Reducer<Text, InvertedIndex, Text, Text> {

    public void reduce(Text key, Iterable<InvertedIndex> values, Context context) throws IOException, InterruptedException {
        final StringBuilder builder = new StringBuilder();
        for (final InvertedIndex val : values) {
            builder.append(val.toString());
            if (values.iterator().hasNext()) {
                builder.append(", ");
            }
        }
        context.write(key, new Text(builder.toString()));
    }
}