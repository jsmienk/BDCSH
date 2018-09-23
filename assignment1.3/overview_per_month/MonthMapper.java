import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

class MonthMapper extends Mapper<LongWritable, Text, IntWritable, IPOccurrence> {

    private static final SimpleDateFormat SDF = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
    private static final Calendar CAL = Calendar.getInstance();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split on whitespace
        final String line = value.toString();
        final String[] partials = line.split("\\s+");

        // Check argument count
        if (partials.length > 3) {

            // Value
            final IPOccurrence ipCount = new IPOccurrence(partials[0], 1);

            // Cutting the '[' of the front
            final String dateString = partials[3].substring(1);

            // Try to parse the date and extract the month
            try {
                // Key
                CAL.setTime(SDF.parse(dateString));
                final IntWritable month = new IntWritable(CAL.get(Calendar.MONTH));

                // Write output
                context.write(month, ipCount);
            } catch (ParseException ignored) {
            }
        }
    }
}