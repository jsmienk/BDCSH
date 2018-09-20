import org.apache.hadoop.mapreduce.Partitioner;

public class MonthPartitioner<K, V> extends Partitioner<K, V> {

    public int getPartition(K key, V value, int numReduceTasks) {

        // Should return a value between 0 and numReduceTasks
        // Based upon the key's value so every key is sent to the right reducer.
        return 0;
    }
}