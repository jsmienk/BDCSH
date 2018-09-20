import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

class IPMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final Text work = new Text(((FileSplit) context.getInputSplit()).getPath().getName());
        final String line = value.toString();

        for (final String word : line.split("\\W+")) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}