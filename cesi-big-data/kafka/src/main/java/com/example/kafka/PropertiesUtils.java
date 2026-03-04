package com.example.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtils {

    private static final Properties PROPS = new Properties();

    static {
        try (InputStream is = PropertiesUtils.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (is == null) throw new IllegalStateException("application.properties introuvable dans le classpath");
            PROPS.load(is);
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static String get(String key) {
        return PROPS.getProperty(key);
    }
}
