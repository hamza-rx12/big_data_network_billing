package me.hamza.springboot_api.controllers;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import me.hamza.springboot_api.DTOs.Customer;
import me.hamza.springboot_api.repository.CustomerRepo;

import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;

@RestController
@RequestMapping("/api/customers")
public class CustomerController {
    private final CustomerRepo customerRepo;

    public CustomerController(CustomerRepo customerRepo) {
        this.customerRepo = customerRepo;
    }

    @GetMapping("/")
    public ResponseEntity<?> getUser() {
        List<Customer> customers = customerRepo.findAll();
        return ResponseEntity.ok(customers);
    }

    @GetMapping("/ids")
    public ResponseEntity<List<Long>> getUserIds() {
        List<Long> ids = customerRepo.findAll()
                .stream()
                .map(Customer::getCustomerId)
                .toList();
        System.out.println("#############IDS RETREIVED##########");
        return ResponseEntity.ok(ids);
    }

}
