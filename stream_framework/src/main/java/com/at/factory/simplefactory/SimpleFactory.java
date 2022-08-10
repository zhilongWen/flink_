package com.at.factory.simplefactory;

/**
 * @create 2022-08-10
 */
public class SimpleFactory {

    public static Product createProduct(String type) {
        if (type == "A") {
            return new ProductA();
        } else {
            return new ProductB();
        }
    }

}
