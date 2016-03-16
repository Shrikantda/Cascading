package com.bitwise.functions;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

public class NormalizeAssembly extends SubAssembly{
	
	public NormalizeAssembly(){
		
	}
	
	public NormalizeAssembly(Pipe inputPipe, Fields inputFields, Fields outputFields){
		
		inputPipe = new Each(inputPipe, inputFields,new NormalizeFunction(outputFields),Fields.RESULTS);
		setTails(inputPipe);
	}

}
