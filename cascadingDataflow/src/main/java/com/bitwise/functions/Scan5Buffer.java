package com.bitwise.functions;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class Scan5Buffer extends BaseOperation implements Buffer {

	
	public Scan5Buffer() {
		
	}
	
	public Scan5Buffer(Fields fieldDeclartion) {
		super(1,fieldDeclartion);
		System.out.println("In Scan Buffer");
	}
	
	@Override
	public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
		System.out.println("In operate method");
		String prev = "*2";
		String current = null;
		TupleEntry group = bufferCall.getGroup();
		TupleEntry entry = null;
		Iterator<TupleEntry> argumentsIterator = bufferCall.getArgumentsIterator();
		Tuple result= new Tuple();
		while(argumentsIterator.hasNext()){
			entry = argumentsIterator.next();
			current = entry.getString(0);
			result = new Tuple();
			result.add(prev);
			result.add(current);
			prev = current;
			bufferCall.getOutputCollector().add(result);
		}
		
	}

}
