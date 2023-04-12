package com.at.factory.simplefactory;

/**
 * @create 2022-08-10
 */
public class Main {

    public static void main(String[] args) {

        String type = "Strategy";

        Product product = SimpleFactory.createProduct(type);

        product.product();

    }

}
