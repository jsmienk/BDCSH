import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

class MonthReducer extends Reducer<Text, CustomValue, Text, CustomValue> {

    public void reduce(Text key, Iterable<CustomValue> values, Context context) throws IOException, InterruptedException {

        // For every month create dictionary of ip's and count ip address
        Iterator<CustomValue> it = values.iterator();
        HashMap<Text, Integer> valueMap = new HashMap<>();

        while(it.hasNext()) {
            CustomValue next = it.next();

            valueMap.putIfAbsent(next.getIp(), 0);
            valueMap.put(next.getIp(), valueMap.get(next.getIp()) + next.getCount().get());
        }

        SortedSet<Text> keys = new TreeSet<>(valueMap.keySet());

        for (Text text : keys) {
            context.write(key, new CustomValue(text, new IntWritable(valueMap.get(text))));
        }

    }
}