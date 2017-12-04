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
package com.marklogic.semantics.rdf4j;

import static org.eclipse.rdf4j.query.QueryLanguage.SPARQL;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.util.Iterator;

import com.marklogic.client.Transaction;
import com.marklogic.semantics.rdf4j.utils.Util;
import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.ExceptionConvertingIteration;
import org.eclipse.rdf4j.common.iteration.Iteration;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.UnsupportedQueryLanguageException;

import org.eclipse.rdf4j.query.impl.SimpleDataset;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.UnknownTransactionStateException;
import org.eclipse.rdf4j.repository.base.AbstractRepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.query.SPARQLQueryBindingSet;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.semantics.GraphPermissions;
import com.marklogic.client.semantics.SPARQLRuleset;
import com.marklogic.semantics.rdf4j.client.MarkLogicClient;
import com.marklogic.semantics.rdf4j.query.MarkLogicBooleanQuery;
import com.marklogic.semantics.rdf4j.query.MarkLogicGraphQuery;
import com.marklogic.semantics.rdf4j.query.MarkLogicQuery;
import com.marklogic.semantics.rdf4j.query.MarkLogicTupleQuery;
import com.marklogic.semantics.rdf4j.query.MarkLogicUpdateQuery;

/**
 * RepositoryConnection to MarkLogic triplestore
 *
 *
 */
public class MarkLogicRepositoryConnection extends AbstractRepositoryConnection implements RepositoryConnection,MarkLogicRepositoryConnectionDependent {

    private static final Logger logger = LoggerFactory.getLogger(MarkLogicRepositoryConnection.class);

    private static final String DEFAULT_GRAPH_URI = "http://marklogic.com/semantics#default-graph";

    private static final String EVERYTHING = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }";

    private static final String SOMETHING = "ASK { ?s ?p ?o }";

    private static final String COUNT_EVERYTHING = "SELECT (count(?s) as ?ct) where { GRAPH ?g { ?s ?p ?o } }";

    private static final String ALL_GRAPH_URIS = "SELECT ?g { GRAPH ?g {} filter (?g != IRI(\"http://marklogic.com/semantics#graphs\"))}";

    private static final String GET_STATEMENTS = "SELECT * {GRAPH ?ctx { ?s ?p ?o . }}";

    private final boolean quadMode;

    private MarkLogicClient client;

    private GraphPermissions defaultGraphPerms;
    private SPARQLRuleset[] defaultRulesets;
    private QueryDefinition defaultQueryDef;

    private Util util = Util.getInstance();

    /**
     * Constructor instantiating MarkLogicRepositoryConnection
     *
     * @param repository
     * @param client
     * @param quadMode
     */
    public MarkLogicRepositoryConnection(MarkLogicRepository repository, MarkLogicClient client, boolean quadMode) {
        super(repository);
        this.client = client;
        this.quadMode = true;
        this.defaultGraphPerms = client.emptyGraphPerms();
        client.setValueFactory(repository.getValueFactory());
    }

    /**
     * Gets the current value factory.
     *
     * @return ValueFactory
     */
    @Override
    public ValueFactory getValueFactory() {
        return client.getValueFactory();
    }

    /**
     * Sets the current value factory.
     *
     * @param f the ValueFactory to be used.
     */
    public void setValueFactory(ValueFactory f) {
        client.setValueFactory(f);
    }


    /**
     * Releases the connection to the database. Ensures that open transactions
     * are complete. Stops write cache timer.
     */
    @Override
    public void close()
            throws RepositoryException
    {
        if(this.isOpen()){
            sync();
            if (this.isActive()) {
                logger.debug("rollback open transaction on closing connection.");
                client.rollbackTransaction();
            }
            client.stopTimer();
            client.close();
            super.close();
        }
    }

    /**
     * Overload for prepareQuery
     *
     * @param queryString Query string to be used.
     * @return MarkLogicQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public Query prepareQuery(String queryString) throws RepositoryException, MalformedQueryException {
        return prepareQuery(QueryLanguage.SPARQL, queryString, null);
    }

    /**
     * Overload for prepareQuery
     *
     * @param queryString Query string to be used.
     * @param baseURI Base URI to be used, with query string.
     * @return MarkLogicQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public Query prepareQuery(String queryString, String baseURI) throws RepositoryException, MalformedQueryException {
        return prepareQuery(QueryLanguage.SPARQL, queryString, baseURI);
    }

    /**
     * Overload for prepareQuery
     *
     * @param queryLanguage  Query language to be used, for the query string.
     * @param queryString Query string to be used
     * @return MarkLogicQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public Query prepareQuery(QueryLanguage queryLanguage, String queryString) throws RepositoryException, MalformedQueryException {
        return prepareQuery(queryLanguage, queryString, null);
    }

    /**
     * Base method for prepareQuery
     *
     * Routes to all other query forms (prepareTupleQuery,prepareBooleanQuery,prepareGraphQuery)
     *
     * @param queryLanguage  Query language to be used, for the query string.
     * @param queryString  Query string to be used.
     * @param baseURI Base URI to be used, with query string.
     * @return MarkLogicQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicQuery prepareQuery(QueryLanguage queryLanguage, String queryString, String baseURI)
            throws RepositoryException, MalformedQueryException
    {
        // function routing based on query form
        if (SPARQL.equals(queryLanguage)) {
            String queryStringWithoutProlog = QueryParserUtil.removeSPARQLQueryProlog(queryString).toUpperCase();
            if (queryStringWithoutProlog.startsWith("SELECT")) {
                return prepareTupleQuery(queryLanguage, queryString, baseURI);   //must be a TupleQuery
            }
            else if (queryStringWithoutProlog.startsWith("ASK")) {
                return prepareBooleanQuery(queryLanguage, queryString, baseURI); //must be a BooleanQuery
            }
            else {
                return prepareGraphQuery(queryLanguage, queryString, baseURI);   //all the rest use GraphQuery
            }
        }
        throw new UnsupportedQueryLanguageException("Unsupported query language " + queryLanguage.getName());
    }

    /**
     * Overload for prepareTupleQuery
     *
     * @param queryString Query string to be used.
     * @return MarkLogicTupleQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicTupleQuery prepareTupleQuery(String queryString) throws RepositoryException, MalformedQueryException {
        return prepareTupleQuery(QueryLanguage.SPARQL, queryString);
    }

    /**
     * Overload for prepareTupleQuery
     *
     * @param queryString Query string to be used.
     * @param baseURI Base URI to be used, with query string.
     * @return MarkLogicTupleQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicTupleQuery prepareTupleQuery(String queryString,String baseURI) throws RepositoryException, MalformedQueryException {
        return prepareTupleQuery(QueryLanguage.SPARQL, queryString, baseURI);
    }

    /**
     * Overload for prepareTupleQuery
     *
     * @param queryLanguage Query language to be used, for the query string.
     * @param queryString Query string to be used.
     * @return MarkLogicTupleQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicTupleQuery prepareTupleQuery(QueryLanguage queryLanguage, String queryString) throws RepositoryException, MalformedQueryException {
        return prepareTupleQuery(queryLanguage, queryString, null);
    }

    /**
     * Base method for prepareTupleQuery
     *
     * @param queryLanguage Query language to be used, for the query string.
     * @param queryString Query string to be used.
     * @param baseURI Base URI to be used, with query string.
     * @return MarkLogicTupleQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicTupleQuery prepareTupleQuery(QueryLanguage queryLanguage, String queryString, String baseURI) throws RepositoryException, MalformedQueryException {
        if (QueryLanguage.SPARQL.equals(queryLanguage)) {
            return new MarkLogicTupleQuery(this.client, new SPARQLQueryBindingSet(), baseURI, queryString, defaultGraphPerms, defaultQueryDef, defaultRulesets);
        }
        throw new UnsupportedQueryLanguageException("Unsupported query language " + queryLanguage.getName());
    }

    /**
     * Overload for prepareGraphQuery
     *
     * @param queryString Query string to be used.
     * @return MarkLogicGraphQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicGraphQuery prepareGraphQuery(String queryString) throws RepositoryException, MalformedQueryException {
        return prepareGraphQuery(QueryLanguage.SPARQL, queryString, null);
    }

    /**
     * Overload for prepareGraphQuery
     *
     * @param queryString Query string to be used.
     * @param baseURI Base URI to be used, with query string.
     * @return MarkLogicGraphQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicGraphQuery prepareGraphQuery(String queryString,String baseURI) throws RepositoryException, MalformedQueryException {
        return prepareGraphQuery(QueryLanguage.SPARQL, queryString, baseURI);
    }

    /**
     * Overload for prepareGraphQuery
     *
     * @param queryLanguage Query language to be used, for the query string.
     * @param queryString Query string to be used.
     * @return MarkLogicGraphQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicGraphQuery prepareGraphQuery(QueryLanguage queryLanguage, String queryString) throws RepositoryException, MalformedQueryException {
        return prepareGraphQuery(queryLanguage, queryString, null);
    }

    /**
     * Base method for prepareGraphQuery
     *
     * @param queryLanguage Query language to be used, for the query string.
     * @param queryString Query string to be used.
     * @param baseURI Base URI to be used, with query string.
     * @return MarkLogicGraphQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicGraphQuery prepareGraphQuery(QueryLanguage queryLanguage, String queryString, String baseURI)
            throws RepositoryException, MalformedQueryException
    {
        if (QueryLanguage.SPARQL.equals(queryLanguage)) {
            return new MarkLogicGraphQuery(this.client, new SPARQLQueryBindingSet(), baseURI, queryString, defaultGraphPerms, defaultQueryDef, defaultRulesets);
        }
        throw new UnsupportedQueryLanguageException("Unsupported query language " + queryLanguage.getName());
    }

    /**
     * Overload for prepareBooleanQuery
     *
     * @param queryString Query string to be used.
     * @return MarkLogicBooleanQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicBooleanQuery prepareBooleanQuery(String queryString) throws RepositoryException, MalformedQueryException {
        return prepareBooleanQuery(QueryLanguage.SPARQL, queryString, null);
    }

    /**
     * Overload for prepareBooleanQuery
     *
     * @param queryString Query string to be used.
     * @param baseURI Base URI to be used, with query string.
     * @return MarkLogicBooleanQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicBooleanQuery prepareBooleanQuery(String queryString,String baseURI) throws RepositoryException, MalformedQueryException {
        return prepareBooleanQuery(QueryLanguage.SPARQL, queryString, baseURI);
    }

    /**
     * Overload for prepareBooleanQuery
     *
     * @param queryLanguage Query language to be used, for the query string.
     * @param queryString Query string to be used.
     * @return MarkLogicBooleanQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicBooleanQuery prepareBooleanQuery(QueryLanguage queryLanguage, String queryString) throws RepositoryException, MalformedQueryException {
        return prepareBooleanQuery(queryLanguage, queryString, null);
    }

    /**
     * Base method for prepareBooleanQuery
     *
     * @param queryLanguage Query language to be used, for the query string.
     * @param queryString Query string to be used.
     * @param baseURI Base URI to be used, with query string.
     * @return MarkLogicBooleanQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicBooleanQuery prepareBooleanQuery(QueryLanguage queryLanguage, String queryString, String baseURI) throws RepositoryException, MalformedQueryException {
        if (QueryLanguage.SPARQL.equals(queryLanguage)) {
            return new MarkLogicBooleanQuery(this.client, new SPARQLQueryBindingSet(), baseURI, queryString, defaultGraphPerms, defaultQueryDef, defaultRulesets);
        }
        throw new UnsupportedQueryLanguageException("Unsupported query language " + queryLanguage.getName());
    }

    /**
     * Overload for prepareUpdate
     *
     * @param queryString Query string to be used.
     * @return MarkLogicUpdateQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicUpdateQuery prepareUpdate(String queryString) throws RepositoryException, MalformedQueryException {
        return prepareUpdate(QueryLanguage.SPARQL, queryString, null);
    }

    /**
     * Overload for prepareUpdate
     *
     * @param queryString Query string to be used.
     * @param baseURI Base URI to be used, with query string.
     * @return MarkLogicUpdateQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicUpdateQuery prepareUpdate(String queryString,String baseURI) throws RepositoryException, MalformedQueryException {
        return prepareUpdate(QueryLanguage.SPARQL, queryString, baseURI);
    }

    /**
     * Overload for prepareUpdate
     *
     * @param queryLanguage Query language to be used, for the query string.
     * @param queryString Query string to be used.
     * @return MarkLogicUpdateQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicUpdateQuery prepareUpdate(QueryLanguage queryLanguage, String queryString) throws RepositoryException, MalformedQueryException {
        return prepareUpdate(queryLanguage, queryString, null);
    }

    /**
     * Base method for prepareUpdate
     *
     * @param queryLanguage Query language to be used, for the query string.
     * @param queryString Query string to be used.
     * @param baseURI Base URI to be used, with query string.
     * @return MarkLogicUpdateQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicUpdateQuery prepareUpdate(QueryLanguage queryLanguage, String queryString, String baseURI) throws RepositoryException, MalformedQueryException {
        if (QueryLanguage.SPARQL.equals(queryLanguage)) {
            return new MarkLogicUpdateQuery(this.client, new SPARQLQueryBindingSet(), baseURI, queryString, defaultGraphPerms, defaultQueryDef, defaultRulesets);
        }
        throw new UnsupportedQueryLanguageException("Unsupported query language " + queryLanguage.getName());
    }

    /**
     * Returns list of graph names as Resource
     *
     * @throws RepositoryException
     */
    @Override
    public RepositoryResult<Resource> getContextIDs() throws RepositoryException {
        try{
            TupleQuery tupleQuery = prepareTupleQuery(QueryLanguage.SPARQL, ALL_GRAPH_URIS);
            TupleQueryResult result = tupleQuery.evaluate();
            return
                    new RepositoryResult<Resource>(
                            new ExceptionConvertingIteration<Resource, RepositoryException>(
                                    new ConvertingIteration<BindingSet, Resource, QueryEvaluationException>(result) {

                                        @Override
                                        protected Resource convert(BindingSet bindings)
                                                throws QueryEvaluationException {
                                            return (Resource) bindings.getValue("g");
                                        }
                                    }) {

                                @Override
                                protected RepositoryException convert(Exception e) {
                                    return new RepositoryException(e);
                                }
                            });

        } catch (MalformedQueryException | QueryEvaluationException e) {
            throw new RepositoryException(e);
        }
    }

    /**
     * Returns all statements.
     *
     * @param subj Subject of the statement.
     * @param pred Predicate of the statement.
     * @param obj Object of the statement.
     * @param includeInferred if false returns only explicit statements. If true returns both explicit and implicit statements.
     * @throws RepositoryException
     */
    public RepositoryResult<Statement> getStatements(Resource subj, IRI pred, Value obj, boolean includeInferred) throws RepositoryException {
        try {
            if (isQuadMode()) {
                TupleQuery tupleQuery = prepareTupleQuery(GET_STATEMENTS);
                setBindings(tupleQuery, subj, pred, obj);
                tupleQuery.setIncludeInferred(includeInferred);
                TupleQueryResult qRes = tupleQuery.evaluate();
                return new RepositoryResult<Statement>(
                        new ExceptionConvertingIteration<Statement, RepositoryException>(
                                toStatementIteration(qRes, subj, pred, obj)) {
                            @Override
                            protected RepositoryException convert(Exception e) {
                                return new RepositoryException(e);
                            }
                        });
            } else if (subj != null && pred != null && obj != null) {
                if (hasStatement(subj, pred, obj, includeInferred)) {
                    ValueFactory vf = SimpleValueFactory.getInstance();
                    Statement st = vf.createStatement(subj, pred, obj);
                    CloseableIteration<Statement, RepositoryException> cursor;
                    cursor = new SingletonIteration<Statement, RepositoryException>(st);
                    return new RepositoryResult<Statement>(cursor);
                } else {
                    return new RepositoryResult<Statement>(new EmptyIteration<Statement, RepositoryException>());
                }
            }
            GraphQuery query = prepareGraphQuery(EVERYTHING);
            query.setIncludeInferred(includeInferred);
            setBindings(query, subj, pred, obj);
            GraphQueryResult result = query.evaluate();
            return new RepositoryResult<Statement>(
                    new ExceptionConvertingIteration<Statement, RepositoryException>(result) {
                        @Override
                        protected RepositoryException convert(Exception e) {
                            return new RepositoryException(e);
                        }
                    });
        } catch (MalformedQueryException | QueryEvaluationException e) {
            throw new RepositoryException(e);
        }
    }

    /**
     * Returns statements from supplied context/s.
     *
     *
     *
     * @param subj Subject of the statement.
     * @param pred Predicate of the statement.
     * @param obj Object of the statement.
     * @param includeInferred if false returns only explicit statements. If true returns both explicit and implicit statements.
     * @param contexts Var-arg for specified contexts.
     * @throws RepositoryException
     */
    // TBD - should share code path with above getStatements
    @Override
    public RepositoryResult<Statement> getStatements(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) throws RepositoryException {
        verifyContextNotNull(contexts);
        try {
            if (isQuadMode()) {
                StringBuilder sb = new StringBuilder();
                if(contexts.length == 0)
                {
                    sb.append(GET_STATEMENTS);
                }
                else {
                    sb.append("SELECT * WHERE { GRAPH ?ctx { ?s ?p ?o } filter (?ctx = (");
                    boolean first = true;
                    for (Resource context : contexts) {
                        if (first) {
                            first = false;
                        } else {
                            sb.append(",");
                        }
                        if (Util.notNull(context)) {
                            sb.append("IRI(\"").append(context.toString()).append("\")");
                        } else {
                            sb.append("IRI(\"" + DEFAULT_GRAPH_URI + "\")");
                        }
                    }
                    sb.append(") ) }");
                }
                TupleQuery tupleQuery = prepareTupleQuery(sb.toString());
                tupleQuery.setIncludeInferred(includeInferred);
                setBindings(tupleQuery, subj, pred, obj, (Resource) null);
                TupleQueryResult qRes = tupleQuery.evaluate();
                return new RepositoryResult<Statement>(
                        new ExceptionConvertingIteration<Statement, RepositoryException>(
                                toStatementIteration(qRes, subj, pred, obj)) {
                            @Override
                            protected RepositoryException convert(Exception e) {
                                return new RepositoryException(e);
                            }
                        });
            } else if (subj != null && pred != null && obj != null) {
                if (hasStatement(subj, pred, obj, includeInferred, contexts)) {
                    ValueFactory vf = SimpleValueFactory.getInstance();
                    Statement st = vf.createStatement(subj, pred, obj);
                    CloseableIteration<Statement, RepositoryException> cursor;
                    cursor = new SingletonIteration<Statement, RepositoryException>(st);
                    return new RepositoryResult<Statement>(cursor);
                } else {
                    return new RepositoryResult<Statement>(new EmptyIteration<Statement, RepositoryException>());
                }
            }
            else {
                MarkLogicGraphQuery query = prepareGraphQuery(EVERYTHING);
                query.setIncludeInferred(includeInferred);
                setBindings(query, subj, pred, obj, contexts);
                GraphQueryResult result = query.evaluate();
                return new RepositoryResult<Statement>(
                        new ExceptionConvertingIteration<Statement, RepositoryException>(result) {
                            @Override
                            protected RepositoryException convert(Exception e) {
                                return new RepositoryException(e);
                            }
                        });
            }
        } catch (MalformedQueryException | QueryEvaluationException e) {
            throw new RepositoryException(e);
        }
    }


    // all statements

    /**
     * Returns true or false if a statement exists in repository / context.
     *
     * @param st The statement to be checked.
     * @param includeInferred if false returns only explicit statements. If true returns both explicit and implicit statements.
     * @param contexts Var-arg for specified contexts.
     * @return boolean
     * @throws RepositoryException
     */
    @Override
    public boolean hasStatement(Statement st, boolean includeInferred, Resource... contexts) throws RepositoryException {
        return hasStatement(st.getSubject(),st.getPredicate(),st.getObject(),includeInferred,contexts);
    }

    /**
     * Returns true or false if a statement exists in repository / context.
     *
     *
     *
     * @param subject Subject of the statement.
     * @param predicate Predicate of the statement.
     * @param object Object of the statement.
     * @param includeInferred if false returns only explicit statements. If true returns both explicit and implicit statements.
     * @param contexts Var-arg for specified contexts.
     * @return boolean
     * @throws RepositoryException
     */
    // TBD- should refactor
    @Override
    public boolean hasStatement(Resource subject, IRI predicate, Value object, boolean includeInferred, Resource... contexts) throws RepositoryException {
        if(!this.isOpen()){throw new RepositoryException("Connection is closed.");}
        String queryString = null;
        verifyContextNotNull(contexts);
    	if (contexts.length == 0) {
            queryString = SOMETHING;
        }
    	else{
    		StringBuilder sb= new StringBuilder();
            sb.append("ASK { GRAPH ?ctx { ?s ?p ?o } filter (?ctx = (");
            boolean first = true;
            for (Resource context : contexts) {
                if (first) {
                    first = false;
                }
                else {
                    sb.append(",");
                }
                if (context == null) {
                    sb.append("IRI(\""+DEFAULT_GRAPH_URI+"\")");
                } else {
                    sb.append("IRI(\"").append(context.toString()).append("\")");
                }
            }
            sb.append(") ) }");
            queryString = sb.toString();
        }
        try {
            logger.debug(queryString);
            MarkLogicBooleanQuery query = prepareBooleanQuery(queryString); // baseuri ?
            query.setIncludeInferred(includeInferred);
            setBindings(query, (Resource) util.skolemize(subject), (IRI) util.skolemize(predicate), util.skolemize(object), contexts);
            return query.evaluate();
        }
        catch (MalformedQueryException | QueryEvaluationException e) {
            throw new RepositoryException(e);
        }
    }

    /**
     * Exports statements via RDFHandler.
     *
     *
     *
     * @param handler RDFHandler
     * @param contexts Var-arg for specified contexts.
     * @throws RepositoryException
     * @throws RDFHandlerException
     */
    // TBD- should refactor
    @Override
    public void export(RDFHandler handler, Resource... contexts) throws RepositoryException, RDFHandlerException {
        exportStatements(null, null, null, false, handler, contexts);
    }

    /**
     * Exports statements via RDFHandler.
     *
     *
     *
     * @param subject Subject of the statement.
     * @param predicate Predicate of the statement.
     * @param object Object of the statement.
     * @param includeInferred if false returns only explicit statements. If true returns both explicit and implicit statements.
     * @param handler RDFHandler
     * @param contexts Var-arg for specified contexts.
     * @throws RepositoryException
     * @throws RDFHandlerException
     */
    // TBD- should refactor
    @Override
    public void exportStatements(Resource subject, IRI predicate, Value object, boolean includeInferred, RDFHandler handler, Resource... contexts) throws RepositoryException, RDFHandlerException {
        try {
            RepositoryResult<Statement> st = this.getStatements(subject, predicate, object, includeInferred, contexts);
            handler.startRDF();
            QueryResults.stream(st).forEach(handler::handleStatement);
            handler.endRDF();
        }
        catch (MalformedQueryException | QueryEvaluationException e) {
            throw new RepositoryException(e);
        }
    }



    /**
     * Returns number of triples in the entire triple store.
     *
     * @return long
     * @throws RepositoryException
     */
    @Override
    public long size() throws RepositoryException{
        try {
            MarkLogicTupleQuery tupleQuery = prepareTupleQuery(COUNT_EVERYTHING);
            tupleQuery.setIncludeInferred(false);
            tupleQuery.setRulesets((SPARQLRuleset)null);
            tupleQuery.setConstrainingQueryDefinition((QueryDefinition)null);
            TupleQueryResult qRes = tupleQuery.evaluate();
            // just one answer
            BindingSet result = qRes.next();
            qRes.close();
            return ((Literal) result.getBinding("ct").getValue()).longValue();
        } catch (QueryEvaluationException | MalformedQueryException e) {
            throw new RepositoryException(e);
        }
    }

    /**
     * Returns number of triples in supplied context.
     *
     * @param contexts Var-arg for specified contexts.
     * @return long
     * @throws RepositoryException
     */
    @Override
    public long size(Resource... contexts) throws RepositoryException {
    	verifyContextNotNull(contexts);
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT (count(?s) as ?ct) where { GRAPH ?g { ?s ?p ?o }");
            boolean first = true;
            // with no args, measure the whole triple store.
            if (contexts != null && contexts.length > 0) {
                sb.append("filter (?g = (");
                for (Resource context : contexts) {
                    if (first) {
                        first = false;
                    }
                    else {
                        sb.append(",");
                    }
                    if (context == null) {
                        sb.append("IRI(\""+DEFAULT_GRAPH_URI+"\")");
                    } else {
                        sb.append("IRI(\"").append(context.toString()).append("\")");
                    }
                }
                sb.append(") )");
            }else{
                sb.append("filter (?g = (IRI(\""+DEFAULT_GRAPH_URI+"\")))");
            }
            sb.append("}");
            logger.debug(sb.toString());
            MarkLogicTupleQuery tupleQuery = prepareTupleQuery(sb.toString());
            tupleQuery.setIncludeInferred(false);
            tupleQuery.setRulesets((SPARQLRuleset) null);
            tupleQuery.setConstrainingQueryDefinition((QueryDefinition)null);
            TupleQueryResult qRes = tupleQuery.evaluate();
            // just one answer
            BindingSet result = qRes.next();
            qRes.close();
            // if 'null' was one or more of the arguments, then totalSize will be non-zero.
            return ((Literal) result.getBinding("ct").getValue()).longValue();
        } catch (QueryEvaluationException | MalformedQueryException e) {
            throw new RepositoryException(e);
        }
    }

    /**
     * Clears all triples from repository.
     *
     * @throws RepositoryException
     */
    @Override
    public void clear() throws RepositoryException{
        getClient().sendClearAll();
    }

    /**
     * Clears triples in supplied context.
     *
     * @param contexts
     * @throws RepositoryException
     */
    @Override
    public void clear(Resource... contexts) throws RepositoryException {
        getClient().sendClear(contexts);
    }

    /**
     * Returns true or false if the repository is empty or not.
     *
     * @return boolean
     * @throws RepositoryException
     */
    @Override
    public boolean isEmpty() throws RepositoryException {
        return size() == 0;
    }

    @Override
    public boolean isOpen() throws RepositoryException {
        return super.isOpen();
    }

    /**
     * Returns true if a transaction is active on connection.
     *
     * @return boolean
     * @throws UnknownTransactionStateException
     * @throws RepositoryException
     */
    @Override
    public boolean isActive() throws UnknownTransactionStateException, RepositoryException {
        return getClient().isActiveTransaction();
    }

    /**
     * Gets the transaction isolation level. (only IsolationLevels.SNAPSHOT supported)
     *
     * @return level
     */
    @Override
    public IsolationLevel getIsolationLevel() {
        return IsolationLevels.SNAPSHOT;
    }

    /**
     * Sets the transaction isolation level. (only IsolationLevels.SNAPSHOT supported)
     *
     * @param level Transaction isolation level.
     * @throws IllegalStateException
     */
    @Override
    public void setIsolationLevel(IsolationLevel level) throws IllegalStateException {
        if(level != IsolationLevels.SNAPSHOT){
            throw new IllegalStateException("Only IsolationLevels.SNAPSHOT level supported.");
        }else{
            super.setIsolationLevel(level);
        }
    }

    /**
     * Opens a new transaction.
     *
     * @throws RepositoryException
     */
    @Override
    public void begin() throws RepositoryException {
        getClient().openTransaction();
    }

    /**
     * Opens a new transaction.
     *
     * @param level
     * @throws RepositoryException
     */
    @Override
    public void begin(IsolationLevel level) throws RepositoryException {
        setIsolationLevel(level);
        begin();
    }

    /**
     * Commits transaction.
     *
     * @throws RepositoryException
     */
    @Override
    public void commit() throws RepositoryException {
        getClient().commitTransaction();
    }

    /**
     * Rollbacks open transaction.
     *
     * @throws RepositoryException
     */
    @Override
    public void rollback() throws RepositoryException {
        getClient().rollbackTransaction();
    }

    /**
     * Add triples via inputstream.
     *
     * @param in the input stream.
     * @param baseURI the baseURI for the input stream.
     * @param dataFormat the data format for the input stream.
     * @param contexts Var-arg for specified context.
     * @throws IOException
     * @throws RDFParseException
     * @throws RepositoryException
     */
    @Override
    public void add(InputStream in, String baseURI, RDFFormat dataFormat, Resource... contexts) throws IOException, RDFParseException, RepositoryException {
    	verifyContextNotNull(contexts);
    	getClient().sendAdd(in, baseURI, dataFormat, contexts);
    }

    /**
     * Add triples via File.
     *
     * It will use file uri as base IRI if none supplied.
     *
     * @param file the file for insertion.
     * @param baseURI the baseURI for the content in the file.
     * @param dataFormat the data format for the file.
     * @param contexts Var-arg for specified context.
     * @throws IOException
     * @throws RDFParseException
     * @throws RepositoryException
     */
    @Override
    public void add(File file, String baseURI, RDFFormat dataFormat, Resource... contexts) throws IOException, RDFParseException, RepositoryException {
        verifyContextNotNull(contexts);
	 	if(Util.notNull(baseURI)) {
            getClient().sendAdd(file, baseURI, dataFormat, contexts);
        }else{
            getClient().sendAdd(file, file.toURI().toString(), dataFormat, contexts);
        }
    }

    /**
     * Add triples via Reader.
     *
     * @param reader the reader as data source.
     * @param baseURI the baseURI for the data source.
     * @param dataFormat the data format for the data source.
     * @param contexts Var-arg for specified context.
     * @throws IOException
     * @throws RDFParseException
     * @throws RepositoryException
     */
    @Override
    public void add(Reader reader, String baseURI, RDFFormat dataFormat, Resource... contexts) throws IOException, RDFParseException, RepositoryException {
    	verifyContextNotNull(contexts);
    	getClient().sendAdd(reader, baseURI, dataFormat, contexts);
    }

    /**
     * Add triples via URL.
     *
     * Sets base IRI to url if none is supplied.
     *
     * @param url the URL for the data.
     * @param baseURI teh base URI for the supplied data.
     * @param dataFormat the data format for the supplied data.
     * @param contexts Var-arg for specified context.
     * @throws IOException
     * @throws RDFParseException
     * @throws RepositoryException
     */
    @Override
    public void add(URL url, String baseURI, RDFFormat dataFormat, Resource... contexts) throws IOException, RDFParseException, RepositoryException {
    	verifyContextNotNull(contexts);
	  	if(Util.notNull(baseURI)) {
            getClient().sendAdd(new URL(url.toString()).openStream(), baseURI, dataFormat, contexts);
        }else{
            getClient().sendAdd(new URL(url.toString()).openStream(), url.toString(), dataFormat, contexts);
        }    
    }

    /**
     * Add single triple statement with supplied context.
     *
     * @param subject Subject of the statement.
     * @param predicate Predicate of the statement.
     * @param object Object of the statement.
     * @param contexts Var-arg for the specified contexts.
     * @throws RepositoryException
     */
    @Override
    public void add(Resource subject, IRI predicate, Value object, Resource... contexts) throws RepositoryException {
    	verifyContextNotNull(contexts);
		getClient().sendAdd(null, subject, predicate, object, contexts);
    }

    /**
     * Add single triple statement with supplied context.
     *
     * @param st Statement to be added.
     * @param contexts Var-arg for the specified contexts.
     * @throws RepositoryException
     */
    @Override
    public void add(Statement st, Resource... contexts) throws RepositoryException {
    	verifyContextNotNull(contexts);
		add(st.getSubject(), st.getPredicate(), st.getObject(), mergeResource(st.getContext(), contexts));
    }

    /**
     * Add triple statements.
     *
     * @param statements Statement to be added.
     * @param contexts Var-arg for the specified contexts.
     * @throws RepositoryException
     */
    @Override
    public void add(Iterable<? extends Statement> statements, Resource... contexts) throws RepositoryException {
    	verifyContextNotNull(contexts);
    	Iterator <? extends Statement> iter = statements.iterator();
	    while(iter.hasNext()){
	    	Statement st = iter.next();
	        add(st, contexts);
	    }
	}

    /**
     * Add triple statements.
     *
     * @param statements Statement to be added.
     * @param contexts Var-arg for the specified contexts.
     * @param <E> Iterator for statement.
     * @throws RepositoryException
     * @throws E
     */
    @Override
    public <E extends Exception> void add(Iteration<? extends Statement, E> statements, Resource... contexts) throws RepositoryException, E {
    	verifyContextNotNull(contexts);
		while(statements.hasNext()){
            Statement st = statements.next();
            add(st.getSubject(), st.getPredicate(), st.getObject(), mergeResource(st.getContext(), contexts));
        }
	}


    /**
     * Remove triple statement.
     *
     * @param subject Subject of the statement.
     * @param predicate Predicate of the statement.
     * @param object Object of the statement.
     * @param contexts Var-arg for the specified contexts.
     * @throws RepositoryException
     */
    @Override
    public void remove(Resource subject, IRI predicate, Value object, Resource... contexts) throws RepositoryException {
    	verifyContextNotNull(contexts);
    	getClient().sendRemove(null, subject, predicate, object, contexts);
    }

    /**
     * Remove triple statement.
     *
     * @param st the statement to be removed.
     * @param contexts Var-arg for the specified contexts.
     * @throws RepositoryException
     */
    @Override
    public void remove(Statement st, Resource... contexts) throws RepositoryException {
    	verifyContextNotNull(contexts);
    	getClient().sendRemove(null, st.getSubject(), st.getPredicate(), st.getObject(), mergeResource(st.getContext(), contexts));
    }

    /**
     * Remove triple statements.
     *
     * @param statements the statement to be removed.
     * @throws RepositoryException
     */
    @Override
    public void remove(Iterable<? extends Statement> statements) throws RepositoryException {
        Iterator <? extends Statement> iter = statements.iterator();
        while(iter.hasNext()){
            Statement st = iter.next();
            getClient().sendRemove(null, st.getSubject(), st.getPredicate(), st.getObject());
        }
    }

    /**
     * Remove triple statements.
     *
     * @param statements the statement to be removed.
     * @param contexts Var-arg for the specified contexts.
     * @throws RepositoryException
     */
    @Override
    public void remove(Iterable<? extends Statement> statements, Resource... contexts) throws RepositoryException {
    	verifyContextNotNull(contexts);
	    Iterator <? extends Statement> iter = statements.iterator();
	    while(iter.hasNext()){
	    	Statement st = iter.next();
	        getClient().sendRemove(null, st.getSubject(), st.getPredicate(), st.getObject(), mergeResource(st.getContext(), contexts));
	     }
	}

    /**
     * Remove triple statements.
     *
     * @param statements the statement to be removed.
     * @param <E> iterator of statements to be removed.
     * @throws RepositoryException
     * @throws E
     */
    @Override
    public <E extends Exception> void remove(Iteration<? extends Statement, E> statements) throws RepositoryException, E {
        while(statements.hasNext()){
            Statement st = statements.next();
            getClient().sendRemove(null, st.getSubject(), st.getPredicate(), st.getObject());
        }
    }

    /**
     * Remove triple statements.
     *
     * @param statements the statement to be removed.
     * @param contexts Var-arg for the specified contexts.
     * @param <E> iterator of statements to be removed.
     * @throws RepositoryException
     * @throws E
     */
    @Override
    public <E extends Exception> void remove(Iteration<? extends Statement, E> statements, Resource... contexts) throws RepositoryException, E {
    	verifyContextNotNull(contexts);
	 	while(statements.hasNext()){
            Statement st = statements.next();
            getClient().sendRemove(null, st.getSubject(), st.getPredicate(), st.getObject(), mergeResource(st.getContext(), contexts));
        }
	}

    /**
     * Add without commit.
     *
     * Supplied to honor interface.
     *
     * @param subject Subject of the statement.
     * @param predicate Predicate of the statement.
     * @param object Object of the statement.
     * @param contexts Var-arg for the specified contexts.
     * @throws RepositoryException
     */
    @Override
    protected void addWithoutCommit(Resource subject, IRI predicate, Value object, Resource... contexts) throws RepositoryException {
    	verifyContextNotNull(contexts);
    	add(subject, predicate, object, contexts); 
    }

    /**
     * Remove without commit.
     *
     * Supplied to honor interface.
     *
     * @param subject Subject of the statement.
     * @param predicate Predicate of the statement.
     * @param object Object of the statement.
     * @param contexts Var-arg for the specified contexts.
     * @throws RepositoryException
     */
    @Override
    protected void removeWithoutCommit(Resource subject, IRI predicate, Value object, Resource... contexts) throws RepositoryException {
    	verifyContextNotNull(contexts);
    	remove(subject, predicate, object, contexts);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // not in scope for 1.0.0 /////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /**
     *
     * Supplied to honor interface.
     *
     * @return
     * @throws RepositoryException
     */
    @Override
    public RepositoryResult<Namespace> getNamespaces() throws RepositoryException {
        return null;
    }

    /**
     *
     * Supplied to honor interface.
     *
     * @param prefix
     * @return
     * @throws RepositoryException
     */
    @Override
    public String getNamespace(String prefix) throws RepositoryException {
        return null;
    }

    /**
     *
     * Supplied to honor interface.
     *
     * @param prefix
     * @param name
     * @throws RepositoryException
     */
    @Override
    public void setNamespace(String prefix, String name) throws RepositoryException {
    }

    /**
     *
     * Supplied to honor interface.
     *
     * @param prefix
     * @throws RepositoryException
     */
    @Override
    public void removeNamespace(String prefix) throws RepositoryException {
    }

    /**
     *
     * Supplied to honor interface.
     *
     * @throws RepositoryException
     */
    @Override
    public void clearNamespaces() throws RepositoryException {
    }
    ///////////////////////////////////////////////////////////////////////////////////////////////


    /**
     * sets default graph permissions to be used by all queries
     *
     * @param graphPerms the graph permissions to be set.
     */
    @Override
    public void setDefaultGraphPerms(GraphPermissions graphPerms) {
        if(Util.notNull(graphPerms)) {
            this.defaultGraphPerms = graphPerms;
        }else{
            this.defaultGraphPerms = client.emptyGraphPerms();
        }
    }

    /**
     * Returns default graph permissions to be used by all queries.
     *
     * @return GraphPermissions
     */
    @Override
    public GraphPermissions getDefaultGraphPerms() {
        return this.defaultGraphPerms;
    }


    /**
     * Sets default QueryDefinition to be used by all queries.
     *
     * @param queryDef the query definition to be used by default.
     */
    @Override
    public void setDefaultConstrainingQueryDefinition(QueryDefinition queryDef) {
        this.defaultQueryDef = queryDef;
        //this.client.setConstrainingQueryDefinition(queryDef);
    }

    /**
     * Returns default QueryDefinition to be used by all queries.
     *
     * @return QueryDefinition
     */
    @Override
    public QueryDefinition getDefaultConstrainingQueryDefinition() {
        return this.defaultQueryDef;
    }

    /**
     * Sets default rulesets to be used by all queries.
     *
     * @param ruleset Var-arg for the default ruleset.
     */
    @Override
    public void setDefaultRulesets(SPARQLRuleset ... ruleset ) {
        //this.defaultRulesets = ruleset;
        this.client.setDefaultRulesets(ruleset);
    }

    /**
     * Returns default rulesets to be used by all queries.
     *
     * @return SPARQLRuleset[]
     */
    @Override
    public SPARQLRuleset[] getDefaultRulesets() {
        return this.client.getDefaultRulesets();
    }


    /**
     * Forces write cache to sync.
     *
     */
    @Override
    public void sync() throws MarkLogicRdf4jException {
        client.sync();
    }

    /**
     * Customise write cache interval and cache size. 
     *
     * @param initDelay - initial interval before write cache is checked
     * @param delayCache - interval (ms) to check write cache
     * @param cacheSize - size (# triples) of write cache
     *
     */
    @Override
    public void configureWriteCache(long initDelay, long delayCache, long cacheSize){
        client.initTimer(initDelay, delayCache,cacheSize);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // private ////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * get client and check if repositoryconnection is open
     *
     * @throws RepositoryException
     */
    private MarkLogicClient getClient() throws RepositoryException{
        if(isOpen()){
            return this.client;
        }else{
            throw new RepositoryException("connection is closed.");
        }
    }

    /**
     * set bindings ?s, ?p and special handling of Value ?o (and ?ctx)
     *
     * @param query
     * @param subj
     * @param pred
     * @param obj
     * @param contexts
     * @throws RepositoryException
     */
    private void setBindings(Query query, Resource subj, IRI pred, Value obj, Resource... contexts)
            throws RepositoryException {
        if (subj != null) {
            query.setBinding("s", subj);
        }
        if (pred != null && pred instanceof IRI) {
            query.setBinding("p", pred);
        }
        if (obj != null) {
            query.setBinding("o", obj);
        }
        verifyContextNotNull(contexts);
        SimpleDataset dataset = new SimpleDataset();
        if(contexts.length > 0){
            for (Resource context : contexts) {
            	if(Util.notNull(context)){
            		if (context instanceof IRI) {
                        dataset.addDefaultGraph((IRI) context);
                    }
            	}
            	else{
            		 dataset.addDefaultGraph(getValueFactory().createIRI(DEFAULT_GRAPH_URI));
            	}
            }
        }
        else {
            dataset.addDefaultGraph(getValueFactory().createIRI(DEFAULT_GRAPH_URI));
        }
        query.setDataset(dataset);
    }

    public Transaction getTransaction()
    {
        return this.client.getTransaction();
    }

    /**
     * return if quadMode is enabled or not (should be)
     *
     * @return
     */
    private boolean isQuadMode() {
        return quadMode;
    }

    /**
     * private utility for merging Resource varargs
     *
     * @param o
     * @param arr
     * @return
     */
    private static Resource[] mergeResource(Resource o, Resource... arr) throws RepositoryException {
    	verifyContextNotNull(arr);
    	if (arr.length == 0 && o != null) {
			arr = new Resource[] { o };
		}
		return arr;          
   }

    /**
     * convert bindings
     *
     * @param iter
     * @param subj
     * @param pred
     * @param obj
     * @return iterator
     */
    private Iteration<Statement, QueryEvaluationException> toStatementIteration(TupleQueryResult iter, final Resource subj, final IRI pred, final Value obj) {
        return new ConvertingIteration<BindingSet, Statement, QueryEvaluationException>(iter) {
            @Override
            protected Statement convert(BindingSet b) throws QueryEvaluationException {
                Resource s = subj==null ? (Resource)b.getValue("s") : subj;
                IRI p = pred==null ? (IRI)b.getValue("p") : pred;
                Value o = obj==null ? b.getValue("o") : obj;
                IRI ctx = (IRI)b.getValue("ctx");
                if (ctx.stringValue().equals(DEFAULT_GRAPH_URI)) {
                    ctx = (IRI) null;
                }
                return getValueFactory().createStatement(s, p, o, ctx);
            }
        };
    }

    
	/**
	 * private utility method that verifies that the supplied contexts parameter is not <tt>null</tt>, throwing an
	 * {@link IllegalArgumentException} if it is.
	 * 
	 * @param contexts
	 *        The parameter to check.
	 * @return Resource[] 
	 * @throws IllegalArgumentException
	 *         If the supplied contexts parameter is <tt>null</tt>.
	 */
	private static void verifyContextNotNull(Resource... contexts) {
		if (contexts == null) {
			throw new IllegalArgumentException(
					"Illegal value null array for contexts argument; either the value should be cast to Resource or an empty array should be supplied");
		}
	}

    ///////////////////////////////////////////////////////////////////////////////////////////////

}