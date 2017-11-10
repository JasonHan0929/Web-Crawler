import java.nio.ByteBuffer;
import java.util.*;

import static java.lang.Math.log;

public class VariableByteCode {

    public byte[] encodeNumber(int n) {
        if (n == 0) {
            return new byte[]{0};
        }
        int i = (int) (log(n) / log(128)) + 1;
        byte[] rv = new byte[i];
        int j = i - 1;
        do {
            rv[j--] = (byte) (n % 128);
            n /= 128;
        } while (j >= 0);
        rv[i - 1] += 128;
        return rv;
    }

    public Queue<int[]> decodeChunk(byte[] byteStream) {
        Queue<int[]> numbers = new LinkedList<>();
        int n = 0;
        int last = 0;
        boolean isId = true;
        boolean notFirst = false;
        int[] pair = new int[2]; // [docID, freq]
        for (byte b : byteStream) {
            if ((b & 0xff) < 128) {
                n = 128 * n + b;
            } else {
                int num;
                if (notFirst) {
                    num = 128 * n + ((b - 128) & 0xff);

                } else {
                    num = 128 * n + ((b - 128) & 0xff);
                    notFirst = true;
                }
                if (isId) {
                    num += last;
                    last = num;
                    pair[0] = num;
                } else {
                    pair[1] = num;
                    numbers.add(Arrays.copyOf(pair, 2));
                }
                isId = !isId;
                n = 0;
            }
        }
        return numbers;
    }


}
