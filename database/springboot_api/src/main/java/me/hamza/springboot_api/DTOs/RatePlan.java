package me.hamza.springboot_api.DTOs;

import java.util.List;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RatePlan {
    private Long ratePlanId;
    private String planName;
    private List<ProductRate> productRates;

}
