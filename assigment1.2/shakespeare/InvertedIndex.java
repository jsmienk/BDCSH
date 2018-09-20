import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class InvertedIndex implements Writable {

    private final Text work;
    private final IntWritable line;
    private final IntWritable count;

    InvertedIndex(final Text work, final IntWritable line) {
        this.work = work;
        this.line = line;
        this.count = new IntWritable(1);
    }

    Text getWork() {
        return work;
    }

    IntWritable getLine() {
        return line;
    }

    IntWritable getCount() {
        return count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        work.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        work.readFields(dataInput);
    }
}