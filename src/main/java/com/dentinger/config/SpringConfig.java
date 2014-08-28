package com.dentinger.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.io.ClassPathResource;

/**
 * Created by dan on 8/20/14.
 */

@Configuration
@ComponentScan({ "com.dentinger.sample", "com.dentinger.dao.ds", "com.dentinger.domain", "com.dentinger.ds" })
public class SpringConfig {

	@Bean
	public static PropertySourcesPlaceholderConfigurer placeHolderConfigurer() {
		final PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer = new PropertySourcesPlaceholderConfigurer();

		propertySourcesPlaceholderConfigurer.setLocation(new ClassPathResource("props.properties"));
		return propertySourcesPlaceholderConfigurer;

	}

}
