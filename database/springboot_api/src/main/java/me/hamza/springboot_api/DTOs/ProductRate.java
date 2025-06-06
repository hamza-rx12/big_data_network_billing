package me.hamza.springboot_api.DTOs;

import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ProductRate {
    private String serviceType; // e.g., "voice", "sms", "data"
    private String unitPrice; // e.g., "0.01 MAD per second"
    private Integer freeUnitsPerCycle; // Optional, e.g., 100 (minutes, SMS, MB, etc.)
    private List<TieredPricing> tieredPricing; // Optional
}
