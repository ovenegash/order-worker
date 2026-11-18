package com.ovenegash.orderworker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.test.context.SpringBootTest;

// necesita docker compose arriba, se corre con INTEGRATION=true
@SpringBootTest
@EnabledIfEnvironmentVariable(named = "INTEGRATION", matches = "true")
class OrderworkerApplicationTests {

	@Test
	void contextLoads() {
	}

}
