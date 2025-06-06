package me.hamza.springboot_api.repository;

import org.springframework.data.mongodb.repository.MongoRepository;

import me.hamza.springboot_api.DTOs.Product;

public interface ProductRepo extends MongoRepository<Product, Integer> {

}
