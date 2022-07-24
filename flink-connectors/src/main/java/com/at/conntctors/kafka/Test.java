package com.at.conntctors.kafka;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * @create 2022-06-01
 */
public class Test {

    public static void main(String[] args) {

        Map<String, Integer> m1 = new HashMap<>();
        m1.put("a", 1);
        m1.put("b", 10);
        m1.put("c", 7);

        Map<String, Integer> m2 = new HashMap<>();
        m2.put("a", 17);
        m2.put("c", 1);
        m2.put("d", 9);


        m1.forEach(new BiConsumer<String, Integer>() {
            @Override
            public void accept(String k, Integer v) {
                m2.merge(k, v, new BiFunction<Integer, Integer, Integer>() {
                    @Override
                    public Integer apply(Integer i1, Integer i2) {
                        System.out.println(k + " -> " + v);
                        System.out.println("i1 = " + i1);
                        System.out.println("i2 = " + i2);
                        return i1 + i2;
                    }
                });
            }
        });

        System.out.println(m1);
        System.out.println(m2);


        System.out.println("=============================================");


        m2
                .values()
                .stream()
                .sorted(
                        new Comparator<Integer>() {
                            @Override
                            public int compare(Integer o1, Integer o2) {
                                return o2.intValue() - o1.intValue();
                            }
                        })
                .limit(3)
                .forEach(r -> System.out.println(m2.get(r) + " -> " + r));


    }

}
