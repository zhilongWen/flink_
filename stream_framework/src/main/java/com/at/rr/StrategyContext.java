package com.at.rr;

import com.at.strategy.v3.AbstractStrategy;
import org.reflections.Reflections;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @create 2023-01-14
 */
public class StrategyContext {

    private static final Map<String, Strategy> registerMap = new HashMap<>();

    static {
        try {

            Reflections reflections = new Reflections(Strategy.class.getPackage().getName());
            Set<Class<? extends Strategy>> subTypesOf = reflections.getSubTypesOf(Strategy.class);

            for (Class<? extends Strategy> aClass : subTypesOf) {
                if (!aClass.isInterface() && !Modifier.isAbstract(aClass.getModifiers())){
                    Strategy strategy = aClass.newInstance();
                    registerMap.put(strategy.getType(), strategy);
                }
            }
        }catch (Exception e){

        }
    }

    // 获取策略
    public static Strategy getStrategy(String rewardType) {
        return registerMap.get(rewardType);
    }


    //public static void registerStrategy(String type, AbstractStrategy abstractStrategy) {
    //
    //}
}
