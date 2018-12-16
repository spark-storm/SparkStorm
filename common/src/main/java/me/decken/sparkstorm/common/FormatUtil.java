package me.decken.sparkstorm.common;

import org.apache.hadoop.util.StringUtils;
import org.slf4j.helpers.MessageFormatter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author decken
 */
public class FormatUtil {

    public static String format(String messagePattern, Object... args) {
        return MessageFormatter.arrayFormat(messagePattern, args).getMessage();
    }

    public static String mapToKv(Map map) {
        checkNotNull(map, "map is null");
        List<String> lines = new ArrayList<>(map.size());
        for (Object k : map.keySet()) {
            lines.add(format("{} => {}", k, map.get(k)));
        }
        return StringUtils.join("\n", lines);
    }


}
