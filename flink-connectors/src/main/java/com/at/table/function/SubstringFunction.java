package com.at.table.function;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @create 2022-09-01
 */
public class SubstringFunction extends ScalarFunction {
    public String eval(String s, Integer begin, Integer end) {
        return s.substring(begin, end);
    }
}
