package csl.dataproc.birch;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;

public interface LongList {
    int size();
    void add(long n);
    void addAll(LongList list);
    long get(int i);

    class ArrayLongList implements LongList, Serializable {
        private long[] data;
        private int count;

        private static float growFactor = 1.5f;

        static final long serialVersionUID = 1L;

        public ArrayLongList() {
            this(10);
        }

        public ArrayLongList(int capacity) {
            data = new long[capacity];
            count = 0;
        }

        public void add(long n) {
            int count = this.count;
            long[] data = this.data;
            if (count >= data.length) {
                data = Arrays.copyOf(data, growSize(count));
                this.data = data;
            }
            data[count] = n;
            ++this.count;
        }

        private int growSize(int count) {
            return Math.max(count + 1, (int) (count * growFactor));
        }

        public int size() {
            return count;
        }

        public void addAll(LongList list) {
            int lCount = list.size();
            long[] data = this.data;
            int currentLen = data.length;
            int nextLen = currentLen;
            int count = this.count;
            while (nextLen - count <= lCount) {
                nextLen = growSize(nextLen);
            }
            if (nextLen != currentLen) {
                data = Arrays.copyOf(data, nextLen);
                this.data = data;
            }
            if (list instanceof ArrayLongList) {
                System.arraycopy(((ArrayLongList) list).data, 0, data, count, lCount);
            } else {
                for (int i = 0; i < lCount; ++i) {
                    data[i] = list.get(i);
                }
            }
            this.count += lCount;
        }

        public long get(int i) {
            return data[i];
        }

        public long[] getData() {
            return data;
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            out.writeInt(count);
            for (int i = 0, c = count; i < c; ++i) {
                out.writeLong(data[i]);
            }
        }
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            count = in.readInt();
            if (data == null) {
                data = new long[count];
            } else if (count >= data.length) {
                data = Arrays.copyOf(data, count);
            }
            for (int i = 0, c = count; i < c; ++i) {
                data[i] = in.readLong();
            }
        }
    }
}
