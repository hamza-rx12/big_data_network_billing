package me.hamza.springboot_api.config;

import com.github.javafaker.Faker;
import me.hamza.springboot_api.DTOs.Customer;
import me.hamza.springboot_api.repository.CustomerRepo;
import me.hamza.springboot_api.repository.ProductRepo;

import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
// import java.util.Locale;
import java.util.Random;

@Configuration
public class DataInitializer {
    private List<Long> customerIds = new ArrayList<>();
    private Faker faker = new Faker();

    @Bean
    CommandLineRunner initDatabase(CustomerRepo customerRepo) {
        return args -> {

            Random random = new Random();
            for (long i = 1; i <= 100; i++) {
                Customer customer = new Customer();
                customer.setCustomerId(generateUniqueId());
                customer.setCustomerName(faker.name().fullName());
                customer.setSubscriptionType("postpaid");
                customer.setRatePlanId(String.valueOf(random.nextInt(5) + 1));
                customer.setActivationDate(LocalDateTime.now()
                        .minusDays(random.nextInt(365))
                        .minusMinutes(random.nextInt(60))
                        .minusSeconds(random.nextInt(60)));
                customer.setStatus(faker.options().option("active", "suspended", "terminated"));
                customer.setRegionOrZone(faker.address().state());
                customerRepo.save(customer);
            }
        };
    }

    @Bean
    CommandLineRunner initProductDatabase(ProductRepo productRepo) {
        return args -> {
            // Voice product
            me.hamza.springboot_api.DTOs.Product voiceProduct = new me.hamza.springboot_api.DTOs.Product();
            voiceProduct.setProductCode(1);
            voiceProduct.setServiceType("voice-calls");
            voiceProduct.setUnit("minute");
            voiceProduct.setRateType("flat");
            productRepo.save(voiceProduct);

            // SMS product
            me.hamza.springboot_api.DTOs.Product smsProduct = new me.hamza.springboot_api.DTOs.Product();
            smsProduct.setProductCode(2);
            smsProduct.setServiceType("SMS-messages");
            smsProduct.setUnit("SMS");
            smsProduct.setRateType("tiered");
            productRepo.save(smsProduct);

            // Data product
            me.hamza.springboot_api.DTOs.Product dataProduct = new me.hamza.springboot_api.DTOs.Product();
            dataProduct.setProductCode(3);
            dataProduct.setServiceType("Data-session-usage");
            dataProduct.setUnit("MB");
            dataProduct.setRateType("time-based");
            productRepo.save(dataProduct);
        };
    }

    public Long generateUniqueId() {
        Long customerId;
        do {
            String customerIdStr = "212"
                    + faker.options().option("6", "7")
                    + faker.number().digits(8);
            customerId = Long.parseLong(customerIdStr);
        } while (customerIds.contains(customerId));
        customerIds.add(customerId);
        return customerId;

    }
}
