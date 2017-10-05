
package ca.nrc.cadc.caom2;

import ca.nrc.cadc.util.HexUtil;
import java.io.Serializable;
import java.security.SecureRandom;

/**
 * CAOM unique ID generator.
 * 
 * @author pdowler
 */
public class CaomIDGenerator implements Serializable {
    private static final long serialVersionUID = 201202141230L;

    private static CaomIDGenerator instance = new CaomIDGenerator();

    private long self;
    private long prev = 0L;

    /**
     * Get the singleton instance of the generator. This is used by CaomEntity constructors to assign IDs.
     * 
     * @return an IDGenerator
     */
    public static CaomIDGenerator getInstance() {
        return instance;
    }

    public CaomIDGenerator() {
        byte[] uniq = getUniqueBytes();
        byte[] s = new byte[8];
        s[0] = uniq[0];
        s[1] = uniq[1];
        this.self = HexUtil.toLong(s);
    }

    public Long generateID() {
        // ID: <2 bytes of self><least significant 6 bytes from clock>
        // 2^16 = 64k prefixes with 2^48ms ~ 8900 years of IDs each
        try {
            Thread.sleep(1L);
        } catch (InterruptedException ignore) {
            // ignore
        }
        long t = System.currentTimeMillis();
        if (t == prev) {
            // enough
            return generateID(); // try again
        }
        prev = t;
        Long ret = new Long(self | ((t << 16) >> 16));
        return ret;
    }

    private byte[] getUniqueBytes() {
        SecureRandom rnd = new SecureRandom();
        byte[] buf = new byte[1024];
        rnd.nextBytes(buf); // random 1k of bytes
        byte[] ret = new byte[2];
        ret[0] = buf[rnd.nextInt(1024)]; // chose a random byte in there
        ret[1] = buf[rnd.nextInt(1024)];
        return ret;
    }
}
