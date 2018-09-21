import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MonthPartitioner<K extends IntWritable, V> extends Partitioner<K, V> {

    @Override
    public int getPartition(final K key, final V value, final int numReduceTasks) {
        return key.get() % numReduceTasks; // Get the remainder to safely pass the key,value on.
    }
}