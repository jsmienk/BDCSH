import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MonthPartitioner<K extends Text, V> extends Partitioner<K, V> {

    public int getPartition(K key, V value, int numReduceTasks) {
        return Integer.parseInt(key.toString()) % numReduceTasks; // Get the remainder to safely pass the key,value on.
    }
}