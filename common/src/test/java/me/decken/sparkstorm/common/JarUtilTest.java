package me.decken.sparkstorm.common;

import me.decken.sparkstorm.common.util.JarUtil;
import org.junit.Test;

import static me.decken.sparkstorm.common.util.FormatUtil.format;
import static org.junit.Assert.assertNotNull;

/**
 * @author decken
 */
public class JarUtilTest {
    @Test
    public void getMavenJarVersion() {
        assertNotNull(JarUtil.getMavenJarVersion("com.google.guava", "guava"));
    }


    @Test(expected = IllegalStateException.class)
    public void guavaJarWithSpark() {
        JarUtil.requireVersion("18.0", JarUtil.getMavenJarVersion("com.google.guava", "guava"));
    }

    @Test(expected = IllegalStateException.class)
    public void junitWithoutPom() {
        JarUtil.requireVersion("4.11", JarUtil.getMavenJarVersion("junit", "junit"));
    }

    @Test
    public void requireCommonsLang3() {
        JarUtil.requireMavenJarVersion("org.apache.commons", "commons-lang3", "3.4");
    }

    @Test
    public void exceptGtActual() {
        JarUtil.requireVersion("1.0", "1.1");
    }

    @Test(expected = IllegalStateException.class)
    public void exceptLtActual() {
        JarUtil.requireVersion("1.3.2", "1.3.1");
    }

    @Test
    public void SameVersion() {
        JarUtil.requireVersion("1.0.0", "1.0.0");
    }

    @Test
    public void exceptGtActualWithMinorVersion() {
        JarUtil.requireVersion("1.1.1", "1.2");
    }

    @Test(expected = IllegalStateException.class)
    public void exceptLtActualWithMinorVersion() {
        JarUtil.requireVersion("1.2.1", "1.2");
    }

    @Test
    public void getExistResourceJarFile() {
        String resourceFileName = "resource.txt";
        String absPath = JarUtil.getJarFileAbsPath(resourceFileName);
        System.out.println(format("resourceFileName:{} 对应的绝对路径:{}", resourceFileName, absPath));
        assertNotNull(absPath);
    }

    @Test(expected = NullPointerException.class)
    public void getNotExistResourceJarFile() {
        String resourceFileName = "not_exist_resource.txt";
        JarUtil.getJarFileAbsPath(resourceFileName);
    }
}