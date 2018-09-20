import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class InvertedIndex implements Writable {

    private Text work;
    private IntWritable line;
    private IntWritable count;

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
    public String toString() {
        return work.toString() + '@' + line.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChars(work.toString());
        out.writeInt(line.get());
        out.writeInt(count.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        work = new Text(in.readLine());
        line = new IntWritable(in.readInt());
        count = new IntWritable(in.readInt());
    }
}