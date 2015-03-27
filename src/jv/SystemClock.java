package jv;

/**
 * Created by raspasov on 12/14/14.
 */
public class SystemClock {
    public static long lastTime = 0;

    /**
     * Returns a strictly increasing time in nanoseconds that passed since Unix Epoch.
     */
    public static synchronized long getTime() {
        long time = System.currentTimeMillis() *1000L*1000L;

        if (time <= lastTime) {
            lastTime++;
            time = lastTime;
        }

        lastTime = time;
        return time;
    }
}