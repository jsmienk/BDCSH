import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class InvertedIndex implements Writable {

    private Text work;
    private IntWritable line;

    // Public empty constructor required for serialization
    public InvertedIndex() {
    }

    InvertedIndex(final Text work, final IntWritable line) {
        this.work = work;
        this.line = line;
    }

    /**
     * Custom toString method to help printing the output in the reducer
     */
    @Override
    public String toString() {
        return work.toString() + '@' + line.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(line.get());
        out.writeUTF(work.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // Read in same order as write
        line = new IntWritable(in.readInt());
        work = new Text(in.readUTF());
    }
}