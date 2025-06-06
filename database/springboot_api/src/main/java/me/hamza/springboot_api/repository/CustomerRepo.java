package me.hamza.springboot_api.repository;

// import java.util.List;

import org.springframework.data.mongodb.repository.MongoRepository;

import me.hamza.springboot_api.DTOs.Customer;

public interface CustomerRepo extends MongoRepository<Customer, Long> {
    // List<Long> findAllCustomerId();
}
