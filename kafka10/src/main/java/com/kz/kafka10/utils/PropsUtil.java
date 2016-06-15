package com.kz.kafka10.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.google.common.io.Resources;

/**
 * Load properties from resources
 *
 */
public class PropsUtil {

	public static Properties loadProps(String propFileName) {
        Properties properties = new Properties();
        try (InputStream propIS = Resources.getResource(propFileName).openStream()) {
            properties.load(propIS);
            return properties;
        } catch (IOException e) {
			throw new RuntimeException("Failed to load properties: "+propFileName, e);
		}
	}
	
	public static void loadProps(Properties props, String propFileName) {
		try {
			InputStream inputStream = ClassLoader.getSystemClassLoader().getResourceAsStream(propFileName);
			props.load(inputStream);
		} catch (IOException e) {
			throw new RuntimeException("Failed to load properties: "+propFileName, e);
		}		
	}
}
