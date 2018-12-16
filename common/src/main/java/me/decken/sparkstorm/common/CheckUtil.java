package me.decken.sparkstorm.common;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author decken
 */
public class CheckUtil {
    public static void checkUnique(Collection objList) {
        Set<Object> set = new HashSet<>(objList.size());
        for (Object o : objList) {
            set.add(o);
        }

        if (set.size() != objList.size()) {
            throw new IllegalArgumentException("element is not unique, list:" + JSON.toJSONString(objList));
        }
    }

    public static void checkSize(Collection a, Collection b) {
        if (a.size() != b.size()) {
            throw new IllegalArgumentException(String.format("two collection size diff, a:%d:%s, b:%d:%s", a.size(), JSON.toJSONString(a), b.size(), JSON.toJSONString(b)));
        }

    }

    public static void checkIsSameSet(Collection a, Collection b) {
        Collection i = CollectionUtils.intersection(a, b);
        HashSet s1 = new HashSet<>(a);
        HashSet s2 = new HashSet<>(b);
        if (i.size() != a.size() || i.size() != b.size()) {
            throw new IllegalArgumentException("the collection is diff\na:" + JSON.toJSONString(a) + "\nb:" + JSON.toJSONString(b) + "\ndiff:" + Sets.difference(s1, s2));
        }
    }

    public static void checkIsBlank(Collection a) {
        for (Object o : a) {
            if (StringUtils.isBlank(o.toString())) {
                throw new IllegalArgumentException("collection has blank element");
            }
        }
    }

    public static void checkNotEmpty(Collection c, Object... args) {
        checkNotEmpty(c, "collection is null or empty", args);
    }

    public static void checkNotEmpty(Collection c) {
        checkNotEmpty(c, "collection is null or empty");
    }

    public static void checkNotEmpty(Collection c, String msg) {
        checkArgument(c != null && !c.isEmpty(), msg);
    }

    public static void checkNotEmpty(Collection c, String msg, Object... args) {
        checkArgument(c != null && !c.isEmpty(), msg, args);
    }

    public static void checkNotBlank(String s) {
        checkArgument(StringUtils.isNotBlank(s), "input string is blank");
    }

    public static void checkNotBlank(String s, String msg) {
        checkArgument(StringUtils.isNotBlank(s), msg);
    }

    public static void checkNotBlank(String s, String msg, Object... args) {
        checkArgument(StringUtils.isNotBlank(s), msg, args);
    }

    public static Boolean isBlank(Collection c) {
        return c == null || c.isEmpty();
    }

}
