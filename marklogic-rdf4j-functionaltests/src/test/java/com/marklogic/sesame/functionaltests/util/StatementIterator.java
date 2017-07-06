package com.marklogic.sesame.functionaltests.util;

import java.util.Iterator;

import org.eclipse.rdf4j.model.Statement;

public class StatementIterator implements Iterator{
	
	private StatementList<Statement> sL;
	private int index = 0;
	private int size ;
	
	public StatementIterator(StatementList<Statement> sL){
		this.sL =sL;
		this.size =sL.size();
	}

	@Override
	public boolean hasNext() {
		if(index < size)
			return true;
		return false;
	}

	@Override
	public Statement next() {
		Statement st =  sL.get(index);
		index ++;
		return st;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}
	
	
}
