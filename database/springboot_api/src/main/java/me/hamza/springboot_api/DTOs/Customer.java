package me.hamza.springboot_api.DTOs;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import lombok.Data;

import java.time.LocalDateTime;

@Data
@Document(collection = "customers")
public class Customer {

    @Id
    private Long customerId;
    private String customerName;
    private String subscriptionType = "postpaid";
    private String ratePlanId;
    private LocalDateTime activationDate;
    private String status; // active, suspended, terminated
    private String regionOrZone;

}
