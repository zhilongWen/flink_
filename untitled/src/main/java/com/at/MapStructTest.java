package com.at;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * @create 2022-09-19
 */
public class MapStructTest {

    // https://www.cnblogs.com/likeguang/p/15491856.html

    @Test
    public void test1() {


        // 单个  字段相同
        Product product = new Product(1, "apple", 3.00);

        ProductVO productVO = ProductMapper.mapper.productVO(product);

        System.out.println(productVO);

        System.out.println();
        System.out.println("==========================================");
        System.out.println();

        // list 字段相同

        Product product1 = new Product(10, "aui", 3.20);

        List<Product> products = Arrays.asList(product, product1);

        List<ProductVO> productVOS = ProductMapper.mapper.productVOList(products);

        System.out.println(productVOS);


    }

    @Test
    public void test2() {


        // 单个 字段不同

        Product product = new Product(99, "appleyui", 3.00);

//        ProductVO2 productVO2 = ProductMapper.mapper.productVO2(product);

//        System.out.println(productVO2);

        System.out.println();
        System.out.println("==========================================");
        System.out.println();

        // list 字段不同

        Product product1 = new Product(10, "aui", 3.20);

        List<Product> products = Arrays.asList(product, product1);

        List<ProductVO2> productVO2s = ProductMapper.mapper.productVO2List(products);

        System.out.println(productVO2s);


    }

}
