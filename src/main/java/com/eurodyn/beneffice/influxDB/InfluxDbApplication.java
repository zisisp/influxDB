package com.eurodyn.beneffice.influxDB;

import com.eurodyn.beneffice.influxDB.service.InfluxDBService;
import lombok.extern.java.Log;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Log
@SpringBootApplication
public class InfluxDbApplication implements ApplicationRunner {

	private final InfluxDBService influxDBService;

	public InfluxDbApplication(InfluxDBService influxDBService) {
		this.influxDBService = influxDBService;
	}

	public static void main(String[] args) {
		SpringApplication.run(InfluxDbApplication.class, args);
	}


	@Override
	public void run(ApplicationArguments args) {
//		DefaultApplicationArguments args1 = new DefaultApplicationArguments("esthesis",
//				"zzzzz", "a839ed04424d0eede3c4223190fb096c",
//				"hardwareId");
		influxDBService.
				run();
	}
}
