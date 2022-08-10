package com.at.factory.abstractfactory;

/**
 * @create 2022-08-10
 */
public class Main {
    public static void main(String[] args) {

        Factory factory = SimpleFactory.create(FactoryA.INSTANCE);

        Product product = factory.createProduct("");

        product.product();

        factory.createAnimal().eat();


    }

}
