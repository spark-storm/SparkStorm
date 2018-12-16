package me.decken.sparkstorm.common;

import scala.collection.JavaConverters;
import scala.collection.Map;
import scala.collection.Seq;

import java.util.List;

/**
 * @author decken
 */
public class CollectionUtil {


    public static <T> Seq<T> javaListToSeq(List<T> list) {
        if (list == null) {
            return null;
        }
        return JavaConverters.asScalaBufferConverter(list).asScala().toSeq();
    }

    public static <T> List<T> seqToJavaList(Seq<T> seq) {
        if (seq == null) {
            return null;
        }
        return JavaConverters.seqAsJavaListConverter(seq).asJava();
    }

    public static <K, V> Map<K, V> javaMapToMap(java.util.Map<K, V> javaMap) {
        return JavaConverters.mapAsScalaMapConverter(javaMap).asScala();
    }

    public static <K, V> java.util.Map<K, V> mapToJavaMap(Map<K, V> map) {
        return JavaConverters.mapAsJavaMapConverter(map).asJava();
    }


    public static Integer[] intListToArray(List<Integer> list) {
        return list.stream().toArray(Integer[]::new);
    }

    public static Integer[] longListToArray(List<Integer> list) {
        return list.stream().toArray(Integer[]::new);
    }

    public static String[] stringListToArray(List<String> list) {
        return list.stream().toArray(String[]::new);
    }

    public static Object[] listToarray(List<Object> list) {
        return list.stream().toArray(Object[]::new);
    }
}
