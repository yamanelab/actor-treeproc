package csl.dataproc.tgls.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** serializable Arrays.asList */
public class ArraysAsList {
    @SafeVarargs
    public static <T> List<T> get(T... args) {
        List<T> l = new ArrayList<>();
        Collections.addAll(l, args);
        return l;
    }
}
