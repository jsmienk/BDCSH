import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

    public static void main(String[] args) throws Exception {
        final Job job = new Job();
        job.setJarByClass(Driver.class);
        job.setJobName("IP Hit Administration");

        job.setMapperClass(IPMapper.class);
//        job.setCombinerClass(IPReducer.class);
        job.setReducerClass(IPReducer.class);

        job.setOutputKeyClass(Text.class);              // an ip address
        job.setOutputValueClass(IntWritable.class);   // the hit count

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}