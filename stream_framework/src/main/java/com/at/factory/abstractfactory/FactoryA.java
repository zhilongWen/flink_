package com.at.factory.abstractfactory;

/**
 * @create 2022-08-10
 */
//public class FactoryA implements Factory{
//    @Override
//    public Product createProduct(String type) {
//        return new ProductA();
//    }
//}


public enum FactoryA implements Factory{

    INSTANCE;

    @Override
    public Product createProduct(String type) {
        return new ProductA();
    }

    @Override
    public AbstractAnimal createAnimal() {
        return new Cat();
    }
}
