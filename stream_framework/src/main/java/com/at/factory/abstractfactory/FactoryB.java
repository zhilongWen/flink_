package com.at.factory.abstractfactory;

/**
 * @create 2022-08-10
 */
//public class FactoryB implements Factory{
//
//
//
//    @Override
//    public Product createProduct(String type) {
//        return new ProductB();
//    }
//}
public enum FactoryB implements Factory{

    INSTANCE;

    @Override
    public Product createProduct(String type) {
        return new ProductB();
    }

    @Override
    public AbstractAnimal createAnimal() {
        return new Dog();
    }
}
