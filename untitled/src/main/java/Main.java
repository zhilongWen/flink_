import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @create 2022-09-22
 */
public class Main {


    @Test
    public void test4(){

        // 如果map中不存在等于"键"的k，则不操作，有才覆盖,跟computeIfAbsent相反

        Map mapIfPresent = new HashMap<String, String>();
        Object valuePresent = mapIfPresent.computeIfPresent("键", (k,v) -> "value");
        System.out.println(mapIfPresent); //输出结果 {}
        System.out.println(valuePresent); //输出结果 null


        mapIfPresent.put("键", "对应的值");

        Object valuePresent2 = mapIfPresent.computeIfPresent("键",(k,v) -> "value2");

        System.out.println(mapIfPresent); //输出结果 {键=value2}
        System.out.println(valuePresent2); //输出结果 value2


    }

    @Test
    public void test3(){

        Map<String,String> map = new HashMap<>();

        //如果map中不存在等于"键"的k，则添加为"键"的k以及"对应的值",如果有，则不操作
        String k1 = map.computeIfAbsent("k1", k -> "V1");
        String k2 = map.computeIfAbsent("k1", k -> "V2");
        String k3 = map.computeIfAbsent("k1", k -> "V3");

        System.out.println(k1);
        System.out.println(k2);
        System.out.println(k3);

    }


    @Test
    public void test2(){

        Map<String,String> map = new HashMap<>();

        String k1 = map.compute("k", (k, v) -> "V1"); // 有值则覆盖，没值则添加，这个跟put一样，但是返回是目前的值，put是返回上次的值
        String k2 = map.compute("k", (k, v) -> "V2");
        String k3 = map.compute("k", (k, v) -> "V3");

        System.out.println(k1);
        System.out.println(k2);
        System.out.println(k3);

    }

    @Test
    public void test1(){

        Map map = new HashMap<String, String>();
        Object put = map.put("键", "对应的值");//如果有值则覆盖，没有则添加。如果没值则返回空，有则返回上次的值
        Object put2 = map.put("键", "对应的值2");//如果有值则覆盖，没有则添加。如果没值则返回空，有则返回上次的值
        Object put3 = map.put("键", "对应的值3");//如果有值则覆盖，没有则添加。如果没值则返回空，有则返回上次的值
        System.out.println(put); //输出结果  null
        System.out.println(put2); //输出结果(上次的值)  对应的值
        System.out.println(put3); //输出结果(上次的值)  对应的值2

    }


}
