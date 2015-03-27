package jv;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by raspasov on 10/4/14.
 */
public class BinaryOps {

    public static int bitShiftLeft (Integer x, Integer y) {

        return x << y;

    }

    public static int unsignedBitShiftRight (Integer x, Integer y)
    {
        return x >>> y;
    }

    public static int bitAnd (Integer x, Integer y) {

        return x & y;

    }

    public static int bitOr (Integer x, Integer y) {

        return x | y;

    }

    public static int bitXOr (Integer x, Integer y) {

        return x ^ y;

    }

    public static AtomicReference createAtomicReference () {
        return new AtomicReference<Thread>();
    }

}
