package com.pablodc.kinesispoc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KinesisPoCApplication {

    public static void main(String[] args) {
        SpringApplication.run(KinesisPoCApplication.class, args);
    }

}
