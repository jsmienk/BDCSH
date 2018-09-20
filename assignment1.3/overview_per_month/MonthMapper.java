import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

class MonthMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, CustomValue> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        final String line = value.toString();
        String[] partials = line.split("\\s+");

        if (partials.length > 4) {

            // Variables to return
            Text date;
            CustomValue ipAndCount = new CustomValue(new Text(partials[0]), new IntWritable(1));

            String d = partials[3] + " " + partials[4]; // We cut the line on whitespace. Putting the date back together
            d = d.substring(1, d.length()-1); // Cut of the brackets '[]' on both sides

            try {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss Z");
                Calendar cal = Calendar.getInstance();
                cal.setTime(df.parse(d));

                date = new Text(Integer.toString(cal.get(Calendar.MONTH))); // Get only the MM part

            } catch (ParseException pex) {
                System.err.println(pex.getMessage());
                System.err.println(line);

                throw new IOException("Line couldn't be processed :(");
            }

            context.write(date, ipAndCount);
        }
    }
}