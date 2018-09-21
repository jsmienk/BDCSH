import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

class MonthReducer extends Reducer<IntWritable, IPOccurrence, IntWritable, IPOccurrence> {

    public void reduce(IntWritable month, Iterable<IPOccurrence> values, Context context) throws IOException, InterruptedException {

        // For every month create dictionary of ip's and count ip address
        final Iterator<IPOccurrence> it = values.iterator();
        final Map<String, Integer> map = new HashMap<>();

        while (it.hasNext()) {
            final IPOccurrence occurrence = it.next();
            final String ipAddress = occurrence.getIp();
            if (!map.containsKey(ipAddress)) {
                map.put(ipAddress, 0);
            }
            map.put(ipAddress, map.get(ipAddress) + occurrence.getCount());
        }

        final SortedSet<String> ipAddresses = new TreeSet<>(map.keySet());
        for (final String ipAddress : ipAddresses) {
            context.write(month, new IPOccurrence(ipAddress, map.get(ipAddress)));
        }
    }
}