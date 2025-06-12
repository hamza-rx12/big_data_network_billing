package me.hamza.springboot_api.config;

import com.github.javafaker.Faker;
import me.hamza.springboot_api.DTOs.Customer;
import me.hamza.springboot_api.repository.CustomerRepo;
import me.hamza.springboot_api.repository.ProductRepo;
import me.hamza.springboot_api.repository.RatePlanRepo;
import me.hamza.springboot_api.DTOs.RatePlan;
import me.hamza.springboot_api.DTOs.ProductRate;
import me.hamza.springboot_api.DTOs.TieredPricing;

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
                customer.setRatePlanId(String.valueOf(random.nextInt(3) + 1));
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

    @Bean
    CommandLineRunner initRatePlans(RatePlanRepo ratePlanRepo) {
        return args -> {
            ////////////////////////////////////////////////////
            ////////////// BASIC PLAN //////////////////////////
            ////////////////////////////////////////////////////
            RatePlan basicPlan = new RatePlan();
            basicPlan.setRatePlanId(1L);
            basicPlan.setPlanName("Basic Plan");

            List<ProductRate> basicRates = new ArrayList<>();

            // Voice rates for basic plan
            ProductRate basicVoice = new ProductRate();
            basicVoice.setServiceType(1); // voice-calls
            basicVoice.setUnitPrice(0.02f); // 0.02 MAD per minute
            basicVoice.setFreeUnitsPerCycle(100);
            basicRates.add(basicVoice);

            // SMS rates for basic plan
            ProductRate basicSms = new ProductRate();
            basicSms.setServiceType(2); // SMS-messages
            basicSms.setUnitPrice(0.50f); // 0.50 MAD per SMS
            basicSms.setFreeUnitsPerCycle(20);
            basicRates.add(basicSms);

            // Data rates for basic plan
            ProductRate basicData = new ProductRate();
            basicData.setServiceType(3); // Data-session-usage
            basicData.setUnitPrice(0.10f); // 0.10 MAD per MB
            basicData.setFreeUnitsPerCycle(300);
            basicRates.add(basicData);

            basicPlan.setProductRates(basicRates);
            ratePlanRepo.save(basicPlan);

            ////////////////////////////////////////////////////
            ////////////// PREMIUM PLAN ////////////////////////
            ////////////////////////////////////////////////////
            RatePlan premiumPlan = new RatePlan();
            premiumPlan.setRatePlanId(2L);
            premiumPlan.setPlanName("Premium Plan");

            List<ProductRate> premiumRates = new ArrayList<>();

            // Voice rates for premium plan
            ProductRate premiumVoice = new ProductRate();
            premiumVoice.setServiceType(1); // voice-calls
            premiumVoice.setUnitPrice(0.01f); // 0.01 MAD per minute
            premiumVoice.setFreeUnitsPerCycle(200);
            premiumRates.add(premiumVoice);

            // SMS rates for premium plan
            ProductRate premiumSms = new ProductRate();
            premiumSms.setServiceType(2); // SMS-messages
            premiumSms.setUnitPrice(0.30f); // 0.30 MAD per SMS
            premiumSms.setFreeUnitsPerCycle(50);
            // Add tiered pricing for SMS
            List<TieredPricing> premiumSmsTiers = new ArrayList<>();
            TieredPricing premiumSmsTier1 = new TieredPricing();
            premiumSmsTier1.setUpToUnits(100);
            premiumSmsTier1.setPricePerUnit(0.30f);
            TieredPricing premiumSmsTier2 = new TieredPricing();
            premiumSmsTier2.setUpToUnits(500);
            premiumSmsTier2.setPricePerUnit(0.25f);
            premiumSmsTiers.add(premiumSmsTier1);
            premiumSmsTiers.add(premiumSmsTier2);
            premiumSms.setTieredPricing(premiumSmsTiers);
            premiumRates.add(premiumSms);

            // Data rates for premium plan
            ProductRate premiumData = new ProductRate();
            premiumData.setServiceType(3); // Data-session-usage
            premiumData.setUnitPrice(0.05f); // 0.05 MAD per MB
            premiumData.setFreeUnitsPerCycle(500);
            // Add tiered pricing for Data
            List<TieredPricing> premiumDataTiers = new ArrayList<>();
            TieredPricing premiumDataTier1 = new TieredPricing();
            premiumDataTier1.setUpToUnits(1000);
            premiumDataTier1.setPricePerUnit(0.05f);
            TieredPricing premiumDataTier2 = new TieredPricing();
            premiumDataTier2.setUpToUnits(5000);
            premiumDataTier2.setPricePerUnit(0.03f);
            premiumDataTiers.add(premiumDataTier1);
            premiumDataTiers.add(premiumDataTier2);
            premiumData.setTieredPricing(premiumDataTiers);
            premiumRates.add(premiumData);

            premiumPlan.setProductRates(premiumRates);
            ratePlanRepo.save(premiumPlan);

            ////////////////////////////////////////////////////
            ////////////// BUSINESS PLAN ///////////////////////
            ////////////////////////////////////////////////////
            RatePlan businessPlan = new RatePlan();
            businessPlan.setRatePlanId(3L);
            businessPlan.setPlanName("Business Plan");

            List<ProductRate> businessRates = new ArrayList<>();

            // Voice rates for business plan
            ProductRate businessVoice = new ProductRate();
            businessVoice.setServiceType(1); // voice-calls
            businessVoice.setUnitPrice(0.015f); // 0.015 MAD per minute
            businessVoice.setFreeUnitsPerCycle(300);
            // Add tiered pricing for Voice
            List<TieredPricing> businessVoiceTiers = new ArrayList<>();
            TieredPricing businessVoiceTier1 = new TieredPricing();
            businessVoiceTier1.setUpToUnits(1000);
            businessVoiceTier1.setPricePerUnit(0.015f);
            TieredPricing businessVoiceTier2 = new TieredPricing();
            businessVoiceTier2.setUpToUnits(5000);
            businessVoiceTier2.setPricePerUnit(0.01f);
            businessVoiceTiers.add(businessVoiceTier1);
            businessVoiceTiers.add(businessVoiceTier2);
            businessVoice.setTieredPricing(businessVoiceTiers);
            businessRates.add(businessVoice);

            // SMS rates for business plan
            ProductRate businessSms = new ProductRate();
            businessSms.setServiceType(2); // SMS-messages
            businessSms.setUnitPrice(0.25f); // 0.25 MAD per SMS
            businessSms.setFreeUnitsPerCycle(300);
            // Add tiered pricing for SMS
            List<TieredPricing> businessSmsTiers = new ArrayList<>();
            TieredPricing businessSmsTier1 = new TieredPricing();
            businessSmsTier1.setUpToUnits(500);
            businessSmsTier1.setPricePerUnit(0.25f);
            TieredPricing businessSmsTier2 = new TieredPricing();
            businessSmsTier2.setUpToUnits(2000);
            businessSmsTier2.setPricePerUnit(0.20f);
            businessSmsTiers.add(businessSmsTier1);
            businessSmsTiers.add(businessSmsTier2);
            businessSms.setTieredPricing(businessSmsTiers);
            businessRates.add(businessSms);

            // Data rates for business plan
            ProductRate businessData = new ProductRate();
            businessData.setServiceType(3); // Data-session-usage
            businessData.setUnitPrice(0.03f); // 0.03 MAD per MB
            businessData.setFreeUnitsPerCycle(1000);
            // Add tiered pricing for Data
            List<TieredPricing> businessDataTiers = new ArrayList<>();
            TieredPricing businessDataTier1 = new TieredPricing();
            businessDataTier1.setUpToUnits(2000);
            businessDataTier1.setPricePerUnit(0.03f);
            TieredPricing businessDataTier2 = new TieredPricing();
            businessDataTier2.setUpToUnits(10000);
            businessDataTier2.setPricePerUnit(0.02f);
            businessDataTiers.add(businessDataTier1);
            businessDataTiers.add(businessDataTier2);
            businessData.setTieredPricing(businessDataTiers);
            businessRates.add(businessData);

            businessPlan.setProductRates(businessRates);
            ratePlanRepo.save(businessPlan);
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
