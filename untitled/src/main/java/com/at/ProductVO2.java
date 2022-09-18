package com.at;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @create 2022-09-19
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProductVO2 {
    private Integer pid;
    private String pname;
    private Double price;
}
