package jv;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by raspasov on 2/12/15.
 */
public class MutableUtils {

    public static Object[] pack(int count, Object[] array, AtomicReference<Thread> edit, int idx) {
        Object[] newArray = new Object[2*(count - 1)];
        int j = 1;
        int bitmap = 0;
        for(int i = 0; i < idx; i++)
            if (array[i] != null) {
                newArray[j] = array[i];
                bitmap |= 1 << i;
                j += 2;
            }
        for(int i = idx + 1; i < array.length; i++)
            if (array[i] != null) {
                newArray[j] = array[i];
                bitmap |= 1 << i;
                j += 2;
            }
        Object[] returnArray = new Object[2];
        returnArray[0] = bitmap;
        returnArray[1] = newArray;
        return returnArray;
    }

}
