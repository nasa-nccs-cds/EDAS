package nasa.nccs.edas.portal;

import java.util.Random;
import java.security.SecureRandom;

public class RandomString {

    private static final char[] symbols;
    private static SecureRandom srandom = null;
    private final char[] sbuf;
    private final byte[] bbuf;

    static {
        StringBuilder tmp = new StringBuilder();
        for (char ch = '0'; ch <= '9'; ++ch)
            tmp.append(ch);
        for (char ch = 'a'; ch <= 'z'; ++ch)
            tmp.append(ch);
        for (char ch = 'A'; ch <= 'Z'; ++ch)
            tmp.append(ch);
        tmp.append("!@#$%^&*()_+`={}[]|;:',./<>?");
        symbols = tmp.toString().toCharArray();
    }

    public RandomString(int length) {
        if (length < 1)
            throw new IllegalArgumentException("length < 1: " + length);
        sbuf = new char[length];
        bbuf = new byte[length];
        srandom = new SecureRandom();
    }

    public String nextString() {
        srandom.nextBytes(bbuf);
        for (int idx = 0; idx < bbuf.length; ++idx)
            sbuf[idx] = symbols[ bbuf[idx] % symbols.length ];
        return new String(sbuf);
    }
}

