package me.hamza.springboot_api.DTOs;

import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ProductRate {
    private Integer serviceType; // 1, 2 or 3
    private Float unitPrice; // e.g., 0.2
    private Integer freeUnitsPerCycle; // Optional, e.g., 100 (minutes, SMS, MB, etc.)
    private List<TieredPricing> tieredPricing; // Optional
}
