import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPair implements WritableComparable<IntPair> {
    private int first;
    private int second;

    @Override
    public String toString() {
        return first + "\t" + second;
    }

    public IntPair() {

    }

    public IntPair(int first, int second) {
        set(first, second);
    }

    public void set(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }

    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
    }

    @Override
    public int hashCode() {
        return first * 163 + second;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof IntPair) {
            IntPair ip = (IntPair) o;
            return first == ip.first && second == ip.second;
        }
        return false;
    }

    public int compareTo(IntPair ip) {
        int cmp = compare(first, ip.first);
        if (cmp != 0) {
            return cmp;
        }
        return compare(second, ip.second);
    }

    public static int compare(int a, int b) {
        return (a < b ? -1 : (a == b ? 0 : 1));
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }
}
