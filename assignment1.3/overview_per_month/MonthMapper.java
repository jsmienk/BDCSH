import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class MonthMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String line = value.toString();

        String[] partials = line.split("\\W+");

        if (partials.length > 0) {
            Text ip = new Text(partials[0]);

            context.write(ip, new IntWritable(1));
        }
    }
}