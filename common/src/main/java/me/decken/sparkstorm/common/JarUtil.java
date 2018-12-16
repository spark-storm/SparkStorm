package me.decken.sparkstorm.common;

import org.apache.hadoop.util.VersionUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;
import static me.decken.sparkstorm.common.CheckUtil.checkNotBlank;

/**
 * @author decken
 */
public class JarUtil {

    public static void requireVersion(String except, String actual) {
        if (VersionUtil.compareVersions(except, actual) > 0) {
            throw new IllegalArgumentException(String.format("期望的版本:%s, 实际的版本:%s", except, actual));
        }
    }

    public static String getMavenJarVersion(String groupId, String artifactId) {
        String sourcePath = "META-INF/maven/" + groupId + "/" + artifactId + "/pom.properties";
        System.out.println(JarUtil.class.getClassLoader().getResource(sourcePath));
        InputStream stream = JarUtil.class.getClassLoader().getResourceAsStream(sourcePath);
        checkNotNull(stream, String.format("%s的数据为空", sourcePath));
        try {
            Properties p = new Properties();
            p.load(stream);
            String version = p.getProperty("version");
            checkNotBlank(version, String.format("%s的version为空", sourcePath));
            return version;
        } catch (IOException e) {
            throw new RuntimeException(String.format("%s 不存在", sourcePath), e);
        }
//        InputStreamReader reader = new InputStreamReader(stream);
//        String data = IOUtils.toString(reader);
//        checkNotBlank(data, String.format("%s内容为空", sourcePath));

    }

}
