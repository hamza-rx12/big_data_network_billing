package me.hamza.springboot_api.repository;

import org.springframework.data.mongodb.repository.MongoRepository;

import me.hamza.springboot_api.DTOs.RatePlan;

public interface RatePlanRepo extends MongoRepository<RatePlan, Long> {

}