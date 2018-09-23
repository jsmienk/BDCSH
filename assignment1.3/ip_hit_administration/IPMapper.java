import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class IPMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split on whitespace
        final String line = value.toString();
        String[] partials = line.split("\\s+");

        if (partials.length > 0) {
            // Write output
            context.write(new Text(partials[0]), new IntWritable(1));
        }
    }
}