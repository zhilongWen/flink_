package com.at.rr;



public class B implements Strategy{

    //public static final B instance = new B();
    //
    //private B(){
    //    register("B");
    //}
    //
    //public static B getInstance() {
    //    return instance;
    //}

    @Override
    public void etl() {
        System.out.println("ETL B");
    }

    @Override
    public String getType() {
        return "B";
    }
}
