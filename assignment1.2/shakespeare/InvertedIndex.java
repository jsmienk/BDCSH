import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class InvertedIndex implements Writable {

    private String work;
    private int line;

    // Public empty constructor required for serialization
    public InvertedIndex() {
    }

    InvertedIndex(final String work, final int line) {
        this.work = work;
        this.line = line;
    }

    /**
     * Custom toString method to help printing the output in the reducer
     */
    @Override
    public String toString() {
        return work + '@' + line;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(line);
        out.writeUTF(work);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // Read in same order as write
        line = in.readInt();
        work = in.readUTF();
    }
}