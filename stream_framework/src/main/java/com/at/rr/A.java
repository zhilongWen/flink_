package com.at.rr;


public class A implements Strategy{

    //public static final A instance = new A();
    //
    //private A(){
    //    register("A");
    //}
    //
    //public static A getInstance() {
    //    return instance;
    //}

    @Override
    public void etl() {
        System.out.println("ETL A");
    }

    @Override
    public String getType() {
        return "A";
    }
}
