package com.at.factory.abstractfactory;

/**
 * @create 2022-08-10
 */
public interface Factory {

    Product createProduct(String type);


    AbstractAnimal createAnimal();


}
