package com.bitwise.functions;

import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

public class ScanAssembly extends SubAssembly{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ScanAssembly(){
		
	}
	
	public ScanAssembly(Pipe inputPipe, Fields groupingFields, Fields sortFields, 
			Fields input, Fields outputFields,Fields passthroughFields){
		
		//setPrevious(getPrevious());
		inputPipe = new GroupBy(inputPipe,groupingFields,sortFields);
		Pipe scanPipe = new Every(inputPipe,input, new Scan5Buffer(outputFields),Fields.SWAP);
		
		setTails(scanPipe);
	}

}
