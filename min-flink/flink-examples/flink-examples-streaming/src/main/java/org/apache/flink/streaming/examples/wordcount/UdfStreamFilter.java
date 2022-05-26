package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * @create 2022-05-23
 */
public class UdfStreamFilter <IN> extends AbstractUdfStreamOperator<IN, FilterFunction<IN>> implements OneInputStreamOperator<IN,IN> {
	public UdfStreamFilter(FilterFunction<IN> userFunction) {
		super(userFunction);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {

	}



}
