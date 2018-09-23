import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MonthPartitioner extends Partitioner<IntWritable, IPOccurrence> {

    @Override
    public int getPartition(final IntWritable key, final IPOccurrence value, final int numReduceTasks) {
        return key.get() % numReduceTasks; // Get the remainder to safely pass the key,value on.
    }
}