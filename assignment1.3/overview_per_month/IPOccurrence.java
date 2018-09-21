import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class IPOccurrence implements Writable {

    private Text ip;
    private IntWritable count;

    public IPOccurrence() {
    }

    IPOccurrence(final Text ip, final IntWritable count) {
        this.ip = ip;
        this.count = count;
    }

    Text getIp() {
        return ip;
    }

    IntWritable getCount() {
        return count;
    }

    @Override
    public String toString() {
        return ip.toString() + "\t" + count.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count.get());
        out.writeUTF(ip.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = new IntWritable(in.readInt());
        ip = new Text(in.readUTF());
    }
}