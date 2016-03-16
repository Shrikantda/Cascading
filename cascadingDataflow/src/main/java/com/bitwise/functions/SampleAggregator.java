package com.bitwise.functions;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class SampleAggregator extends BaseOperation implements Aggregator {

	int min;
	int max;
	
	public SampleAggregator(Fields fieldsDeclaration) {
		super(2,fieldsDeclaration);
	}
	
	@Override
	public void start(FlowProcess flowProcess, AggregatorCall aggregatorCall) {
		min = Integer.MAX_VALUE;
		max = Integer.MIN_VALUE;
		
	}

	@Override
	public void aggregate(FlowProcess flowProcess, AggregatorCall aggregatorCall) {
		TupleEntry entry = aggregatorCall.getArguments();
		if(entry.getInteger(0) < min){
			min = entry.getInteger(0);
		}
		if(entry.getInteger(1)>max){
			max = entry.getInteger(1);
		}	
	}

	@Override
	public void complete(FlowProcess flowProcess, AggregatorCall aggregatorCall) {
		Tuple result = new Tuple();
		result.add(min);
		result.add(max);
		
		aggregatorCall.getOutputCollector().add(result);
		
	}

}
