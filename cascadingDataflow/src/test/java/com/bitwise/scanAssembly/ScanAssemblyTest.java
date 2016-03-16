package com.bitwise.scanAssembly;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Test;

import com.bitwise.functions.ScanAssembly;
import com.hotels.plunger.Bucket;
import com.hotels.plunger.Data;
import com.hotels.plunger.DataBuilder;
import com.hotels.plunger.Plunger;

import cascading.pipe.Pipe;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class ScanAssemblyTest {
	
	@Test
	public void TestSimpleScanOpertion(){
		
		Plunger plunger = new Plunger();
		
		Data file = new DataBuilder(new Fields("id","name","score_original","address"))
				.addTuple("1","ad","23","abc")
				.addTuple("2","bb","24","def")
				.addTuple("3","df","40","ghi")
				.addTuple("1","fsgs","43","jkl")
				.addTuple("2","dfg","65","afd")
				.addTuple("2","fsgs","67","aghdd")
				.addTuple("3","fdggs","85","hwrw")
				.addTuple("1","adty","34","utow")
				.build();
		
		Pipe newPipe = plunger.newNamedPipe("namedPipe", file);
		
		Fields inputFields = new Fields("score_original");
		Fields outputFields = new Fields("score_old","score_new");
		Fields passthroughFields = new Fields("id","name","address");
		Fields groupingFields = new Fields("id");
		Fields sortFields = new Fields("score_original");
		
		
		/*Pipe resultPipe = new SumBy(newPipe,groupingFields,new Fields("score_original"), new Fields("sum"),Integer.class);
		Bucket bucket1 = plunger.newBucket(new Fields("sum"), resultPipe);
		List<Tuple> result1 = bucket1.result().asTupleList();
		assertThat(result1.size(), is(1));*/
		
	    newPipe = new ScanAssembly(newPipe, groupingFields, sortFields, inputFields, outputFields, passthroughFields);
		Bucket bucket = plunger.newBucket(new Fields("id","name","score_old","score_new","address"), newPipe);
		List<Tuple> result = bucket.result().asTupleList();
		assertThat(result.size(), is(8));
		
		
	}

	
}
