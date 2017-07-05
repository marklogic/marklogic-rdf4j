package com.marklogic.sesame.functionaltests.util;

import java.util.Iterator;

import org.eclipse.rdf4j.model.Statement;

public class StatementIterable implements Iterable{ 

	private StatementIterator sI;
	
	@Override
	public Iterator<Statement> iterator() {
		// TODO Auto-generated method stub
		return this.sI;
	}
	
	public StatementIterable(StatementIterator  sI){
		this.sI =sI;
	}
}
