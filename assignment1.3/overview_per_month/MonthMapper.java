import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

class MonthMapper extends Mapper<LongWritable, Text, IntWritable, IPOccurrence> {

    private static final IntWritable ONE = new IntWritable(1);
    private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss Z");
    private static final Calendar CAL = Calendar.getInstance();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split on whitespace
        final String line = value.toString();
        final String[] partials = line.split("\\s+");

        // Check argument count
        if (partials.length > 4) {

            // Value
            final IPOccurrence ipCount = new IPOccurrence(new Text(partials[0]), ONE);

            // We had cut the line on whitespace. Putting the date back together
            final StringBuilder builder = new StringBuilder(partials[3]);
            builder.append(' ').append(partials[4]);
            // Cut of the brackets '[]' on both sides
            final String dateString = builder.substring(1, builder.length() - 1);

            // Try to parse the date and extract the month
            try {
                // Key
                CAL.setTime(SDF.parse(dateString));
                final IntWritable date = new IntWritable(CAL.get(Calendar.MONTH));

                // Write output
                context.write(date, ipCount);
            } catch (ParseException pex) {
                System.err.println(pex.getMessage());
                System.err.println(line);
            }
        }
    }
}