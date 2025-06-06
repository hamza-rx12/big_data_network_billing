package me.hamza.springboot_api.DTOs;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TieredPricing {
    private Integer upToUnits; // e.g., 100 (MB, minutes, etc.)
    private String pricePerUnit; // e.g., "0.05 MAD/MB"

}
