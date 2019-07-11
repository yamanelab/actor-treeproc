package csl.dataproc.dot;

import java.util.ArrayList;
import java.util.List;

public class DotAttributeList {
    List<DotAttribute> list = new ArrayList<DotAttribute>();

    public DotAttributeList add(DotAttribute a) {
        DotAttribute attr = get(a.key);
        if (attr != null) {
            list.set(list.indexOf(attr), a);
        } else {
            list.add(a);
        }
        return this;
    }

    public DotAttributeList add(String key, String val) {
        return add(new DotAttribute(key, val));
    }

    public DotAttributeList addQuote(String key, String val) {
        return add(new DotAttribute(key, val).quote());
    }
    
    public List<DotAttribute> getList() {
        return list;
    }

    public String toString() {
        StringBuffer buf = new StringBuffer();
        if (!list.isEmpty()) {
            buf.append("[");
            for (int i = 0, l = list.size(); i < l; ++i) {
                if (i != 0) {
                    buf.append(",");
                }
                buf.append(list.get(i));
            }
            buf.append("]");
        }
        return buf.toString();
    }

    public DotAttribute get(String key) {
        for (DotAttribute a : list) {
            if (a.key.equals(key)) {
                return a;
            }
        }
        return null;
    }
}