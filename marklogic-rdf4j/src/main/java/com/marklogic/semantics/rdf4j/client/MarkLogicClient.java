/*
 * Copyright 2015-2017 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * A library that enables access to a MarkLogic-backed triple-store via the
 * RDF4J API.
 */
package com.marklogic.semantics.rdf4j.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.ForbiddenUserException;
import com.marklogic.client.Transaction;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.semantics.GraphPermissions;
import com.marklogic.client.semantics.SPARQLRuleset;
import com.marklogic.semantics.rdf4j.MarkLogicRdf4jException;
import com.marklogic.semantics.rdf4j.MarkLogicTransactionException;
import com.marklogic.semantics.rdf4j.utils.Util;
import org.apache.commons.io.input.ReaderInputStream;
import org.eclipse.rdf4j.http.protocol.UnauthorizedException;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.resultio.QueryResultIO;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultParser;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sparql.query.SPARQLQueryBindingSet;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.helpers.ParseErrorLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Timer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * An internal class that straddles Rdf4j and MarkLogic Java client API.
 *
 * @author James Fuller
 */
public class MarkLogicClient {

	private static final Logger logger = LoggerFactory.getLogger(MarkLogicClient.class);

	protected static final Charset UTF8 = Charset.forName("UTF-8");
	protected static final Charset charset = UTF8;

	protected static final TupleQueryResultFormat format = TupleQueryResultFormat.JSON;
	protected static final RDFFormat rdfFormat = RDFFormat.NTRIPLES;
	private MarkLogicClientImpl _client;

	private final Executor executor = Executors.newCachedThreadPool();

	private ValueFactory f;

	private ParserConfig parserConfig = new ParserConfig();

	private Transaction tx = null;

    private SPARQLRuleset[] defaultRulesets;

    private TripleWriteCache timerWriteCache;
	private Timer writeTimer;
	private TripleDeleteCache timerDeleteCache;
	private Timer deleteTimer;

	private static boolean WRITE_CACHE_ENABLED = true;
	private static boolean DELETE_CACHE_ENABLED = false;

	private Util util = Util.getInstance();

	/**
	 * Constructor initialized with connection parameters.
	 *
	 */
	public MarkLogicClient(String host, int port, String user, String password, String database, String auth) {
		this._client = new MarkLogicClientImpl(host, port, user, password, database, auth);
		this.initTimer();
	}

	/**
	 * Constructor initialized with DatabaseClient.
	 *
	 */
	public MarkLogicClient(DatabaseClient databaseClient) {
		this._client = new MarkLogicClientImpl(databaseClient);
		this.initTimer();
	}

	/**
	 * start Timer task (write cache)
	 */
	public void initTimer(){
		stopTimer();
		if(WRITE_CACHE_ENABLED) {
			logger.debug("configuring write cache with defaults");
			timerWriteCache = new TripleWriteCache(this);
			writeTimer = new Timer();
			writeTimer.scheduleAtFixedRate(timerWriteCache, TripleWriteCache.DEFAULT_INITIAL_DELAY, TripleWriteCache.DEFAULT_CACHE_MILLIS);
		}
		if(DELETE_CACHE_ENABLED) {
			logger.debug("configuring delete cache with defaults");
			timerDeleteCache = new TripleDeleteCache(this);
			deleteTimer = new Timer();
			deleteTimer.scheduleAtFixedRate(timerDeleteCache, TripleDeleteCache.DEFAULT_INITIAL_DELAY, TripleDeleteCache.DEFAULT_CACHE_MILLIS);
		}
	}

	public void initTimer(long initDelay, long delayCache, long cacheSize ){
		stopTimer();
		if(WRITE_CACHE_ENABLED) {
			logger.debug("configuring write cache");
			timerWriteCache = new TripleWriteCache(this,cacheSize);
			writeTimer = new Timer();
			writeTimer.scheduleAtFixedRate(timerWriteCache, initDelay, delayCache);
		}
		if(DELETE_CACHE_ENABLED) {
			logger.debug("configuring delete cache");
			timerDeleteCache = new TripleDeleteCache(this);
			deleteTimer = new Timer();
			deleteTimer.scheduleAtFixedRate(timerDeleteCache, initDelay, delayCache);
		}
	}
	/**
	 * stop Timer task (write cache)
	 */
	public void stopTimer() {
		if(WRITE_CACHE_ENABLED) {
			if(timerWriteCache != null) {
				timerWriteCache.cancel();
			}
			if(writeTimer != null){
				writeTimer.cancel();
			}
			if(timerDeleteCache != null) {
				timerDeleteCache.cancel();
			}
			if(deleteTimer != null){
				deleteTimer.cancel();
			}
		}
	}

	/**
	 *  Forces write cache to flush triples.
	 *
	 * @throws MarkLogicRdf4jException
	 */
	public void sync() throws MarkLogicRdf4jException {
		if(WRITE_CACHE_ENABLED && timerWriteCache != null)
			timerWriteCache.forceRun();
		if(DELETE_CACHE_ENABLED && timerDeleteCache != null)
			timerDeleteCache.forceRun();
	}

	/**
	 * get value factory
	 *
	 * @return ValueFactory
	 */
	public ValueFactory getValueFactory() {
		return this.f;
	}

	/**
	 * sets the value factory
	 *
	 * @param f
	 */
	public void setValueFactory(ValueFactory f) {
		this.f=f;
	}

	/**
	 * TupleQuery
	 *
	 * @param queryString
	 * @param bindings
	 * @param start
	 * @param pageLength
	 * @param includeInferred
	 * @param baseURI
	 * @return
	 * @throws RepositoryException
	 * @throws MalformedQueryException
	 * @throws UnauthorizedException
	 * @throws QueryInterruptedException
	 */
	public TupleQueryResult sendTupleQuery(String queryString,SPARQLQueryBindingSet bindings, long start, long pageLength, boolean includeInferred, String baseURI) throws RepositoryException, MalformedQueryException,
			QueryInterruptedException {
		InputStream stream = null;
		try {
			stream = getClient().performSPARQLQuery(queryString, bindings, start, pageLength, this.tx, includeInferred, baseURI);
		} catch (JsonProcessingException e) {
			logger.error(e.getLocalizedMessage());
			throw new MarkLogicRdf4jException("Issue processing json.");
		}
		TupleQueryResultParser parser = QueryResultIO.createTupleParser(format, getValueFactory());
		MarkLogicBackgroundTupleResult tRes = new MarkLogicBackgroundTupleResult(parser,stream);
		execute(tRes);
		return tRes;
	}

	/**
	 * GraphQuery
	 *
	 * @param queryString
	 * @param bindings
	 * @param includeInferred
	 * @param baseURI
	 * @return
	 * @throws IOException
	 */
	public GraphQueryResult sendGraphQuery(String queryString, SPARQLQueryBindingSet bindings, boolean includeInferred, String baseURI) throws IOException, MarkLogicRdf4jException {
		InputStream stream = getClient().performGraphQuery(queryString, bindings, this.tx, includeInferred, baseURI);

		RDFParser parser = Rio.createParser(rdfFormat, getValueFactory());
		parser.setParserConfig(getParserConfig());
		parser.setParseErrorListener(new ParseErrorLogger());
		parser.setPreserveBNodeIDs(true);

		MarkLogicBackgroundGraphResult gRes;

		// fixup - baseURI cannot be null
		if(baseURI != null){
			gRes= new MarkLogicBackgroundGraphResult(parser,stream,charset,baseURI);
		}else{
			gRes= new MarkLogicBackgroundGraphResult(parser,stream,charset,"");
		}

		execute(gRes);
		return gRes;
	}

	/**
	 * BooleanQuery
	 *
	 * @param queryString
	 * @param bindings
	 * @param includeInferred
	 * @param baseURI
	 * @return
	 * @throws IOException
	 * @throws RepositoryException
	 * @throws MalformedQueryException
	 * @throws UnauthorizedException
	 * @throws QueryInterruptedException
	 */
	public boolean sendBooleanQuery(String queryString, SPARQLQueryBindingSet bindings, boolean includeInferred, String baseURI) throws IOException, RepositoryException, MalformedQueryException,
			QueryInterruptedException {
		return getClient().performBooleanQuery(queryString, bindings, this.tx, includeInferred, baseURI);
	}

	/**
	 * UpdateQuery
	 *
	 * @param queryString
	 * @param bindings
	 * @param includeInferred
	 * @param baseURI
	 * @throws IOException
	 * @throws RepositoryException
	 * @throws MalformedQueryException
	 * @throws UnauthorizedException
	 * @throws UpdateExecutionException
	 */
	public void sendUpdateQuery(String queryString, SPARQLQueryBindingSet bindings, boolean includeInferred, String baseURI) throws IOException, RepositoryException, MalformedQueryException,UpdateExecutionException {
		getClient().performUpdateQuery(queryString, bindings, this.tx, includeInferred, baseURI);
	}

	/**
	 * Add triples from file.
	 *
	 * @param file
	 * @param baseURI
	 * @param dataFormat
	 * @param contexts
	 * @throws RDFParseException
	 */
	public void sendAdd(File file, String baseURI, RDFFormat dataFormat, Resource... contexts) throws RDFParseException {
        if(util.isFormatSupported(dataFormat)) {
            getClient().performAdd(file, baseURI, dataFormat, this.tx, contexts);
        }
        else
        {
            throw new MarkLogicRdf4jException("Unsupported RDF format.");
        }
	}

	/**
	 * Add triples from InputStream.
	 *
	 * @param in
	 * @param baseURI
	 * @param dataFormat
	 * @param contexts
	 */
	public void sendAdd(InputStream in, String baseURI, RDFFormat dataFormat, Resource... contexts) throws RDFParseException, MarkLogicRdf4jException {
        if(util.isFormatSupported(dataFormat)) {
            getClient().performAdd(in, baseURI, dataFormat, this.tx, contexts);
        }
        else
        {
            throw new MarkLogicRdf4jException("Unsupported RDF format.");
        }
	}

	/**
	 * Add triples from Reader.
	 *
	 * @param in
	 * @param baseURI
	 * @param dataFormat
	 * @param contexts
	 */
	public void sendAdd(Reader in, String baseURI, RDFFormat dataFormat, Resource... contexts) throws RDFParseException, MarkLogicRdf4jException {
        if(util.isFormatSupported(dataFormat)) {
            //TBD- must deal with char encoding
            getClient().performAdd(new ReaderInputStream(in, Charset.defaultCharset()), baseURI, dataFormat, this.tx, contexts);
        }
        else
        {
            throw new MarkLogicRdf4jException("Unsupported RDF format.");
        }
	}

	/**
	 * Add single triple, if cache is enabled will add triple to cache model.
	 *
	 * @param baseURI
	 * @param subject
	 * @param predicate
	 * @param object
	 * @param contexts
	 */
	public void sendAdd(String baseURI, Resource subject, IRI predicate, Value object, Resource... contexts) throws MarkLogicRdf4jException {
		if (WRITE_CACHE_ENABLED) {
			timerWriteCache.add((Resource) util.skolemize(subject), (IRI) util.skolemize(predicate), util.skolemize(object), contexts);
		} else {
			getClient().performAdd(baseURI, (Resource) util.skolemize(subject), (IRI) util.skolemize(predicate), util.skolemize(object), this.tx, contexts);
		}
	}

	/**
	 * Remove single triple.
	 *
	 * @param baseURI
	 * @param subject
	 * @param predicate
	 * @param object
	 * @param contexts
	 */
	public void sendRemove(String baseURI, Resource subject, IRI predicate, Value object, Resource... contexts) throws MarkLogicRdf4jException {
		if (DELETE_CACHE_ENABLED) {
			timerDeleteCache.add((Resource) util.skolemize(subject), (IRI) util.skolemize(predicate), util.skolemize(object), contexts);
		} else {
			if (WRITE_CACHE_ENABLED)
				sync();
			getClient().performRemove(baseURI, (Resource) util.skolemize(subject), (IRI) util.skolemize(predicate), util.skolemize(object), this.tx, contexts);
		}
	}

	/**
	 * Clears all triples from context.
	 *
	 * @param contexts
	 */
	public void sendClear(Resource... contexts) throws MarkLogicRdf4jException {
		getClient().performClear(this.tx, contexts);
	}

	/**
	 * Clear all triples.
	 *
	 */
	public void sendClearAll() throws MarkLogicRdf4jException {
		getClient().performClearAll(this.tx);
	}

	/**
	 * Opens a transaction.
	 *
	 * @throws MarkLogicTransactionException
	 */
	public void openTransaction() throws MarkLogicTransactionException {
		if (!isActiveTransaction()) {
			try {
                this.tx = getClient().getDatabaseClient().openTransaction();
            }
            catch (ForbiddenUserException e)
            {
                throw new RepositoryException(e.getMessage());
            }
		}else{
			throw new MarkLogicTransactionException("Only one active transaction allowed.");
		}
	}

	/**
	 * Commits a transaction.
	 *
	 * @throws MarkLogicTransactionException
	 */
	public void commitTransaction() throws MarkLogicTransactionException {
		if (isActiveTransaction()) {
			try {
				sync();
				this.tx.commit();
				this.tx=null;
			} catch (MarkLogicRdf4jException e) {
				logger.error(e.getLocalizedMessage());
				throw new MarkLogicTransactionException(e);
			}
		}else{
			throw new MarkLogicTransactionException("No active transaction to commit.");
		}
	}

	/**
	 * Rollback transaction.
	 *
	 * @throws MarkLogicTransactionException
	 */
	public void rollbackTransaction() throws MarkLogicTransactionException {
		if(isActiveTransaction()) {
			try {
				sync();
			} catch (MarkLogicRdf4jException e) {
				throw new MarkLogicTransactionException(e);
			}
			this.tx.rollback();
			this.tx = null;
		}else{
			throw new MarkLogicTransactionException("No active transaction to rollback.");
		}
	}

	/**
	 * Checks if a transaction currently exists.
	 *
	 *
	 */
	public boolean isActiveTransaction(){
		return this.tx != null;
	}

	/**
	 * sets transaction (tx) to null
	 *
	 * @throws MarkLogicTransactionException
	 */
	public void setAutoCommit() throws MarkLogicTransactionException {
		if (isActiveTransaction()) {
			throw new MarkLogicTransactionException("Active transaction.");
		}else{
			this.tx=null;
		}
	}

	/**
	 * getter for ParserConfig
	 *
	 * @return
	 */
	public ParserConfig getParserConfig() {
		return this.parserConfig;
	}

	/**
	 * setter for ParserConfig
	 *
	 * @param parserConfig
	 */
	public void setParserConfig(ParserConfig parserConfig) {
		this.parserConfig=parserConfig;
	}

	/**
	 * setter for Rulesets
	 *
	 * @param rulesets
	 */
	public void setRulesets(SPARQLRuleset... rulesets){
	    if(this.defaultRulesets != null)
        {
            if(rulesets != null)
            {
                SPARQLRuleset[] resultantRuleset = Arrays.copyOf(rulesets, rulesets.length + defaultRulesets.length);
                System.arraycopy(defaultRulesets, 0, resultantRuleset, rulesets.length, defaultRulesets.length);
                getClient().setRulesets(resultantRuleset);
            }
            else
            {
                getClient().setRulesets(this.defaultRulesets);
            }
        }
        else
        {
            getClient().setRulesets(rulesets);
        }
	}

	/**
	 * getter for Rulesets
	 *
	 * @return
	 */
	public SPARQLRuleset[] getRulesets(){
		return getClient().getRulesets();
	}

	public void setDefaultRulesets(SPARQLRuleset... rulesets)
    {
        this.defaultRulesets = rulesets;
    }

	public SPARQLRuleset[] getDefaultRulesets() {
		return this.defaultRulesets;
	}

	/**
	 * setter for QueryDefinition
	 *
	 * @param constrainingQueryDefinition
	 */
	public void setConstrainingQueryDefinition(QueryDefinition constrainingQueryDefinition){
		getClient().setConstrainingQueryDefinition(constrainingQueryDefinition);
	}

	/**
	 * getter for QueryDefinition
	 *
	 * @return
	 */
	public QueryDefinition getConstrainingQueryDefinition(){
		return getClient().getConstrainingQueryDefinition();
	}

	/**
	 * setter for GraphPermissions
	 *
	 * @param graphPerms
	 */
	public void setGraphPerms(GraphPermissions graphPerms){

		if (graphPerms != null) {
			getClient().setGraphPerms(graphPerms);
		}else {
			getClient().setGraphPerms(getClient().getDatabaseClient().newGraphManager().newGraphPermissions());
		}
	}

	/**
	 * getter for GraphPermissions
	 *
	 * @return
	 */
	public GraphPermissions getGraphPerms(){
		return getClient().getGraphPerms();
	}

	public GraphPermissions emptyGraphPerms(){
		return _client.getDatabaseClient().newGraphManager().newGraphPermissions();
	}

	/**
	 * Execute command.
	 * @param command
	 */
	protected void execute(Runnable command) {
		executor.execute(command);
	}


	///////////////////////////////////////////////////////////////////////////////////////////////
	// private ////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 *
	 * @return
	 */
	private MarkLogicClientImpl getClient(){
		return this._client;
	}

	/**
	 *
	 */
	public void close() {
		_client.close();
	}
	/**
	 *
	 */
	public void release() {
		_client.release();
	}
}