import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class IPOccurrence implements Writable {

    private String ip;
    private int count;

    public IPOccurrence() {
    }

    IPOccurrence(final String ip, final int count) {
        this.ip = ip;
        this.count = count;
    }

    String getIp() {
        return ip;
    }

    int getCount() {
        return count;
    }

    @Override
    public String toString() {
        return ip + "\t" + count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
        out.writeUTF(ip);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readInt();
        ip = in.readUTF();
    }
}