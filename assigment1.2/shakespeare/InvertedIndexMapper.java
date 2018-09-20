import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, InvertedIndex> {

    private static final IntWritable ONE = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final Text work = new Text(((FileSplit) context.getInputSplit()).getPath().getName());
        final String line = value.toString();

        IntWritable lineNumber = null;
        for (final String word : line.split("\\W+")) {
            if (lineNumber == null) {
                lineNumber = new IntWritable(1);
            }
            if (word.length() > 0) {
                context.write(new Text(word), new InvertedIndex());
            }
        }
    }
}