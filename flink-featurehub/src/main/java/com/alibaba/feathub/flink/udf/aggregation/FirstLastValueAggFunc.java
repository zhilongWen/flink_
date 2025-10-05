/*
 * Copyright 2022 The FeatHub Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.feathub.flink.udf.aggregation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.LinkedList;

import static com.alibaba.feathub.flink.udf.aggregation.AggFuncUtils.insertIntoSortedList;
import static com.alibaba.feathub.flink.udf.aggregation.AggFuncUtils.mergeSortedLists;

/** Aggregate function that get the first value or last value. */
public class FirstLastValueAggFunc<T>
        implements AggFunc<T, T, FirstLastValueAggFunc.FirstLastValueAccumulator<T>> {

    private final DataType inDataType;
    protected final boolean isFirstValue;

    public FirstLastValueAggFunc(DataType inDataType, boolean isFirstValue) {
        this.inDataType = inDataType;
        this.isFirstValue = isFirstValue;
    }

    @Override
    public void add(FirstLastValueAccumulator<T> acc, T value, long timestamp) {
        if (acc.rawDataList.isEmpty() || timestamp >= acc.rawDataList.getLast().f1) {
            acc.rawDataList.add(Tuple2.of(value, timestamp));
            return;
        }

        insertIntoSortedList(
                acc.rawDataList, Tuple2.of(value, timestamp), (o1, o2) -> (int) (o1.f1 - o2.f1));
    }

    @Override
    public void merge(FirstLastValueAccumulator<T> target, FirstLastValueAccumulator<T> source) {
        if (source.rawDataList.isEmpty()) {
            return;
        }

        if (target.rawDataList.isEmpty()) {
            target.rawDataList.addAll(source.rawDataList);
            return;
        }

        target.rawDataList =
                mergeSortedLists(
                        target.rawDataList, source.rawDataList, (o1, o2) -> (int) (o1.f1 - o2.f1));
    }

    @Override
    public void retract(FirstLastValueAccumulator<T> accumulator, T value) {
        Preconditions.checkState(
                accumulator.rawDataList.getFirst().f0.equals(value),
                "Value must be retracted by the ordered as they added to the FirstLastValueAggFuncBase.");
        accumulator.rawDataList.removeFirst();
    }

    @Override
    public void retractAccumulator(
            FirstLastValueAccumulator<T> target, FirstLastValueAccumulator<T> source) {
        for (Tuple2<T, Long> value : source.rawDataList) {
            Preconditions.checkState(
                    target.rawDataList.getFirst().equals(value),
                    "Value must be retracted by the order as they added to the AggFuncWithLimit.");
            target.rawDataList.removeFirst();
        }
    }

    @Override
    public T getResult(FirstLastValueAccumulator<T> accumulator) {
        if (accumulator.rawDataList.isEmpty()) {
            return null;
        }
        if (isFirstValue) {
            return accumulator.rawDataList.getFirst().f0;
        } else {
            return accumulator.rawDataList.getLast().f0;
        }
    }

    @Override
    public DataType getResultDatatype() {
        return inDataType;
    }

    @Override
    public FirstLastValueAccumulator<T> createAccumulator() {
        return new FirstLastValueAccumulator<>();
    }

    @Override
    public TypeInformation getAccumulatorTypeInformation() {
        return Types.POJO(FirstLastValueAccumulator.class);
    }

    /** Accumulator that collects raw data and their timestamps. */
    public static class FirstLastValueAccumulator<T> {
        public LinkedList<Tuple2<T, Long>> rawDataList = new LinkedList<>();
    }
}
