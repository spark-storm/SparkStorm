package me.decken.sparkstorm.common;

import org.apache.hadoop.util.VersionUtil;
import org.spark_project.jetty.util.StringUtil;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;
import static me.decken.sparkstorm.common.FormatUtil.format;

/**
 * @author decken
 */
public class JarUtil {

    /**
     * @param exceptVersion 期望的版本号
     * @param actualVersion 实际的版本号
     * @throws IllegalStateException 如果except低于actual的版本, 则抛出该异常
     */
    public static void requireVersion(String exceptVersion, String actualVersion) {
        if (VersionUtil.compareVersions(exceptVersion, actualVersion) > 0) {
            throw new IllegalStateException(format("期望的版本:{}, 实际的版本:{}", exceptVersion, actualVersion));
        }
    }

    public static void requireMavenJarVersion(String groupId, String artifactId, String exceptVersion) {
        requireVersion(getMavenJarVersion(groupId, artifactId), exceptVersion);
    }


    /**
     * 从maven的打包规范的pom.properties提取出jar包的版本号
     * 使用maven打包出来的jar包,会在jar包目录下的META-INF/maven/{groupId}/{artifactId}生成pom.properties,
     * 里面保存了对应jar包的版本信息
     * <p><p>
     * 该方法并不是万能的, 在一些场景下可能会失效.
     * 比如一些jar包使用shade的方式打包, 将包名重命名之后,但是并没有将pom.properties的路径进行重命名,用户又自己引入不同版本的jar包, 这样pom.properties就会覆盖掉, 这种场景下依靠该方法来判断会失误.
     * <p><p>
     * 典型的例子就是在spark2.3.1(之前的也会)环境下用户指定了非14.0.1的版本后使用该方法判断guava版本的时候会失误. 原因是spark-network-common里面使用了guava14.0.1, 这个jar包以shade打包并改了guava对应的包路径,这样来避免类冲突, 但是pom.properties的路径依旧在com.google.guava#guava下, 如果用户在spark环境下使用了更高版本的guava, 由于spark自己的jar放在jars目录下, 会优先加载, 用户使用的高版本的pom.properties并不会覆盖spark里面打包进来的.
     *
     * @param groupId    maven的groupId
     * @param artifactId maven的artifactId
     * @return pom.properties里面version对应的值
     * @throws IllegalStateException 无法通过该方法判断时, 抛出该异常
     */
    public static String getMavenJarVersion(String groupId, String artifactId) {
        String sourcePath = "META-INF/maven/" + groupId + "/" + artifactId + "/pom.properties";

        URL absPath = JarUtil.class.getClassLoader().getResource(sourcePath);
        System.out.println(format("{}:{} jarPath:{}", groupId, artifactId, absPath));
        InputStream stream = JarUtil.class.getClassLoader().getResourceAsStream(sourcePath);
        if (stream == null) {
            throw new IllegalStateException(format("{}的数据为空, 无法通过该方法判断", sourcePath));
        }

        try {
            Properties p = new Properties();
            p.load(stream);
            String version = p.getProperty("version");
            if (StringUtil.isBlank(version)) {
                throw new IllegalStateException(format("{}的version为空", sourcePath));
            }
            return version;
        } catch (IOException e) {
            throw new IllegalStateException(format("{} 不存在, 无法通过该方法判断", sourcePath), e);
        }
    }

    /**
     * @param sourcePath
     * @return
     */
    public static String getJarFileAbsPath(String sourcePath) {
        checkNotNull(sourcePath, "source不能为null");
        URL url = JarUtil.class.getClassLoader().getResource(sourcePath);
        checkNotNull(url, format("根据path:{} 找出来的url为null", sourcePath));
        return url.toString();
    }

}
