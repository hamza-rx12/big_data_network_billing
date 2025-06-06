package me.hamza.springboot_api.DTOs;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import lombok.Data;

// import java.util.List;

@Data
@Document(collection = "products")
public class Product {

    @Id
    private Integer productCode;
    private String serviceType;
    private String unit;
    private String rateType;
    // private List<String> telecomServices;

    // public Product(String productCode, String serviceType, String unit, String
    // rateType, List<String> telecomServices) {
    // this.productCode = productCode;
    // this.serviceType = serviceType;
    // this.unit = unit;
    // this.rateType = rateType;
    // this.telecomServices = telecomServices;
    // }

}