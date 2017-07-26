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

import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.semantics.GraphPermissions;
import com.marklogic.client.semantics.SPARQLRuleset;
import com.marklogic.semantics.rdf4j.query.*;
import com.marklogic.semantics.rdf4j.client.MarkLogicClient;
import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.common.iteration.*;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.StatementImpl;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.impl.SimpleDataset;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLUtil;
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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.util.Iterator;

import static org.eclipse.rdf4j.query.QueryLanguage.SPARQL;

/**
 * RepositoryConnection to MarkLogic triplestore
 *
 * @author James Fuller
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

    /**
     * constructor
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
     * gets the current value factory
     *
     * @return ValueFactory
     */
    @Override
    public ValueFactory getValueFactory() {
        return client.getValueFactory();
    }

    /**
     * sets the current value factory
     *
     * @param f
     */
    public void setValueFactory(ValueFactory f) {
        client.setValueFactory(f);
    }


    @Override
    /**
     * Releases the connection to the database.  Ensures that open transactions
     * are complete. Stops write cache Timer.
     */
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
     * overload for prepareQuery
     *
     * @param queryString
     * @return MarkLogicQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public Query prepareQuery(String queryString) throws RepositoryException, MalformedQueryException {
        return prepareQuery(QueryLanguage.SPARQL, queryString, null);
    }

    /**
     * overload for prepareQuery
     *
     * @param queryString
     * @param baseURI
     * @return MarkLogicQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public Query prepareQuery(String queryString, String baseURI) throws RepositoryException, MalformedQueryException {
        return prepareQuery(QueryLanguage.SPARQL, queryString, baseURI);
    }

    /**
     * overload for prepareQuery
     *
     * @param queryLanguage
     * @param queryString
     * @return MarkLogicQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public Query prepareQuery(QueryLanguage queryLanguage, String queryString) throws RepositoryException, MalformedQueryException {
        return prepareQuery(queryLanguage, queryString, null);
    }

    /**
     * base method for prepareQuery
     *
     * routes to all other query forms (prepareTupleQuery,prepareBooleanQuery,prepareGraphQuery)
     *
     * @param queryLanguage
     * @param queryString
     * @param baseURI
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
     * overload for prepareTupleQuery
     *
     * @param queryString
     * @return MarkLogicTupleQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicTupleQuery prepareTupleQuery(String queryString) throws RepositoryException, MalformedQueryException {
        return prepareTupleQuery(QueryLanguage.SPARQL, queryString);
    }

    /**
     * overload for prepareTupleQuery
     *
     * @param queryString
     * @param baseURI
     * @return MarkLogicTupleQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicTupleQuery prepareTupleQuery(String queryString,String baseURI) throws RepositoryException, MalformedQueryException {
        return prepareTupleQuery(QueryLanguage.SPARQL, queryString, baseURI);
    }

    /**
     * overload for prepareTupleQuery
     *
     * @param queryLanguage
     * @param queryString
     * @return MarkLogicTupleQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicTupleQuery prepareTupleQuery(QueryLanguage queryLanguage, String queryString) throws RepositoryException, MalformedQueryException {
        return prepareTupleQuery(queryLanguage, queryString, null);
    }

    /**
     * base method for prepareTupleQuery
     *
     * @param queryLanguage
     * @param queryString
     * @param baseURI
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
     * overload for prepareGraphQuery
     *
     * @param queryString
     * @return MarkLogicGraphQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicGraphQuery prepareGraphQuery(String queryString) throws RepositoryException, MalformedQueryException {
        return prepareGraphQuery(QueryLanguage.SPARQL, queryString, null);
    }

    /**
     * overload for prepareGraphQuery
     *
     * @param queryString
     * @param baseURI
     * @return MarkLogicGraphQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicGraphQuery prepareGraphQuery(String queryString,String baseURI) throws RepositoryException, MalformedQueryException {
        return prepareGraphQuery(QueryLanguage.SPARQL, queryString, baseURI);
    }

    /**
     * overload for prepareGraphQuery
     *
     * @param queryLanguage
     * @param queryString
     * @return MarkLogicGraphQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicGraphQuery prepareGraphQuery(QueryLanguage queryLanguage, String queryString) throws RepositoryException, MalformedQueryException {
        return prepareGraphQuery(queryLanguage, queryString, null);
    }

    /**
     * base method for prepareGraphQuery
     *
     * @param queryLanguage
     * @param queryString
     * @param baseURI
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
     * overload for prepareBooleanQuery
     *
     * @param queryString
     * @return MarkLogicBooleanQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicBooleanQuery prepareBooleanQuery(String queryString) throws RepositoryException, MalformedQueryException {
        return prepareBooleanQuery(QueryLanguage.SPARQL, queryString, null);
    }

    /**
     * overload for prepareBooleanQuery
     *
     * @param queryString
     * @param baseURI
     * @return MarkLogicBooleanQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicBooleanQuery prepareBooleanQuery(String queryString,String baseURI) throws RepositoryException, MalformedQueryException {
        return prepareBooleanQuery(QueryLanguage.SPARQL, queryString, baseURI);
    }

    /**
     * overload for prepareBooleanQuery
     *
     * @param queryLanguage
     * @param queryString
     * @return MarkLogicBooleanQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicBooleanQuery prepareBooleanQuery(QueryLanguage queryLanguage, String queryString) throws RepositoryException, MalformedQueryException {
        return prepareBooleanQuery(queryLanguage, queryString, null);
    }

    /**
     * base method for prepareBooleanQuery
     *
     * @param queryLanguage
     * @param queryString
     * @param baseURI
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
     * overload for prepareUpdate
     *
     * @param queryString
     * @return MarkLogicUpdateQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicUpdateQuery prepareUpdate(String queryString) throws RepositoryException, MalformedQueryException {
        return prepareUpdate(QueryLanguage.SPARQL, queryString, null);
    }

    /**
     * overload for prepareUpdate
     *
     * @param queryString
     * @param baseURI
     * @return MarkLogicUpdateQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicUpdateQuery prepareUpdate(String queryString,String baseURI) throws RepositoryException, MalformedQueryException {
        return prepareUpdate(QueryLanguage.SPARQL, queryString, baseURI);
    }

    /**
     * overload for prepareUpdate
     *
     * @param queryLanguage
     * @param queryString
     * @return MarkLogicUpdateQuery
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    @Override
    public MarkLogicUpdateQuery prepareUpdate(QueryLanguage queryLanguage, String queryString) throws RepositoryException, MalformedQueryException {
       return prepareUpdate(queryLanguage, queryString, null);
    }

    /**
     * base method for prepareUpdate
     *
     * @param queryLanguage
     * @param queryString
     * @param baseURI
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
     * returns list of graph names as Resource
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

        } catch (MalformedQueryException e) {
            throw new RepositoryException(e);
        } catch (QueryEvaluationException e) {
            throw new RepositoryException(e);
        }
    }

    /**
     * returns all statements
     *
     * @param subj
     * @param pred
     * @param obj
     * @param includeInferred
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
                    Statement st = new StatementImpl(subj, pred, obj);
                    CloseableIteration<Statement, RepositoryException> cursor;
                    cursor = new SingletonIteration<Statement, RepositoryException>(st);
                    return new RepositoryResult<Statement>(cursor);
                } else {
                    return new RepositoryResult<Statement>(new EmptyIteration<Statement, RepositoryException>());
                }
            }
            GraphQuery query = prepareGraphQuery(EVERYTHING);
            setBindings(query, subj, pred, obj);
            GraphQueryResult result = query.evaluate();
            return new RepositoryResult<Statement>(
                    new ExceptionConvertingIteration<Statement, RepositoryException>(result) {
                        @Override
                        protected RepositoryException convert(Exception e) {
                            return new RepositoryException(e);
                        }
                    });
        } catch (MalformedQueryException e) {
            throw new RepositoryException(e);
        } catch (QueryEvaluationException e) {
            throw new RepositoryException(e);
        }
    }

    /**
     * returns statements from supplied context
     *
     * TBD - should share code path with above getStatements
     *
     * @param subj
     * @param pred
     * @param obj
     * @param includeInferred
     * @param contexts
     * @throws RepositoryException
     */
    @Override
    public RepositoryResult<Statement> getStatements(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) throws RepositoryException {
        if (contexts == null) {
            contexts = new Resource[] { null };
        }
        try {
            if (isQuadMode()) {
                StringBuilder sb = new StringBuilder();
                sb.append("SELECT * WHERE { GRAPH ?ctx { ?s ?p ?o } filter (?ctx = (");
                boolean first = true;
                for (Resource context : contexts) {
                    if (first) {
                        first = !first;
                    }
                    else {
                        sb.append(",");
                    }
                    if (notNull(context)) {
                        sb.append("IRI(\"" + context.toString() + "\")");
                    } else {
                        sb.append("IRI(\""+DEFAULT_GRAPH_URI+"\")");
                    }
                }
                sb.append(") ) }");
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
                    Statement st = new StatementImpl(subj, pred, obj);
                    CloseableIteration<Statement, RepositoryException> cursor;
                    cursor = new SingletonIteration<Statement, RepositoryException>(st);
                    return new RepositoryResult<Statement>(cursor);
                } else {
                    return new RepositoryResult<Statement>(new EmptyIteration<Statement, RepositoryException>());
                }
            }
            else {
                MarkLogicGraphQuery query = prepareGraphQuery(EVERYTHING);
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
        } catch (MalformedQueryException e) {
            throw new RepositoryException(e);
        } catch (QueryEvaluationException e) {
            throw new RepositoryException(e);
        }
    }

    // all statements

    /**
     * returns true or false if a statement exists in repository / context
     *
     * @param st
     * @param includeInferred
     * @param contexts
     * @return boolean
     * @throws RepositoryException
     */
    @Override
    public boolean hasStatement(Statement st, boolean includeInferred, Resource... contexts) throws RepositoryException {
        return hasStatement(st.getSubject(),st.getPredicate(),st.getObject(),includeInferred,contexts);
    }

    /**
     * returns true or false if a statement exists in repository / context
     *
     * TBD- should refactor
     *
     * @param subject
     * @param predicate
     * @param object
     * @param includeInferred
     * @param contexts
     * @return boolean
     * @throws RepositoryException
     */
    @Override
    public boolean hasStatement(Resource subject, IRI predicate, Value object, boolean includeInferred, Resource... contexts) throws RepositoryException {
        if(!this.isOpen()){throw new RepositoryException("Connection is closed.");}
        String queryString = null;
        if(contexts == null) {
            queryString="ASK { GRAPH ?ctx { ?s ?p ?o } filter (?ctx = (IRI(\""+DEFAULT_GRAPH_URI+"\")))}";
        }else if (contexts.length == 0) {
            queryString = SOMETHING;
        }
        else {
            StringBuilder sb= new StringBuilder();
            sb.append("ASK { GRAPH ?ctx { ?s ?p ?o } filter (?ctx = (");
                boolean first = true;
                for (Resource context : contexts) {
                    if (first) {
                        first = !first;
                    }
                    else {
                        sb.append(",");
                    }
                    if (context == null) {
                        sb.append("IRI(\""+DEFAULT_GRAPH_URI+"\")");
                    } else {
                        sb.append("IRI(\"" + context.toString() + "\")");
                    }
                }
                sb.append(") ) }");
            queryString = sb.toString();
        }
        try {
            logger.debug(queryString);
            MarkLogicBooleanQuery query = prepareBooleanQuery(queryString); // baseuri ?

            setBindings(query, subject, predicate, object, contexts);
            return query.evaluate();
        }
        catch (MalformedQueryException e) {
            throw new RepositoryException(e);
        }
        catch (QueryEvaluationException e) {
            throw new RepositoryException(e);
        }
    }

    /**
     * exports statements via RDFHandler
     *
     * TBD- should refactor
     *
     * @param handler
     * @param contexts
     * @throws RepositoryException
     * @throws RDFHandlerException
     */
    @Override
    public void export(RDFHandler handler, Resource... contexts) throws RepositoryException, RDFHandlerException {
        exportStatements(null, null, null, true, handler);
    }

    /**
     * exports statements via RDFHandler
     *
     * TBD- should refactor
     *
     * @param subject
     * @param predicate
     * @param object
     * @param includeInferred
     * @param handler
     * @param contexts
     * @throws RepositoryException
     * @throws RDFHandlerException
     */
    @Override
    public void exportStatements(Resource subject, IRI predicate, Value object, boolean includeInferred, RDFHandler handler, Resource... contexts) throws RepositoryException, RDFHandlerException {
        try {
            StringBuilder ob = new StringBuilder();
            StringBuilder sb = new StringBuilder();

            if(notNull(object) && object instanceof Literal) {
                if (object instanceof Literal) {
                    Literal lit = (Literal) object;
                    ob.append("\"");
                    ob.append(SPARQLUtil.encodeString(lit.getLabel()));
                    ob.append("\"");
                    ob.append("^^<" + lit.getDatatype().stringValue() + ">");
                    ob.append(" ");
                } else {
                    ob.append("<" + object.stringValue() + "> ");
                }
                sb.append("CONSTRUCT {?s ?p "+ob.toString()+"} WHERE {");
                if(notNull(contexts) && contexts.length>0) {
                    for (int i = 0; i < contexts.length; i++) {
                        if(notNull(contexts[i])) {
                            sb.append("GRAPH <" + contexts[i].stringValue() + "> {?s ?p " + ob.toString() + " .} ");
                        }else{
                            sb.append("GRAPH <"+DEFAULT_GRAPH_URI+"> {?s ?p "+ob.toString()+" .}");
                        }
                    }
                    sb.append("}");
                }else{
                    sb.append("?s ?p "+ob.toString()+" }");
                }
            }else{
                sb.append("CONSTRUCT {?s ?p ?o} WHERE {");
                if(notNull(contexts) && contexts.length>0) {
                    for (int i = 0; i < contexts.length; i++) {
                        if(contexts[i] != null) {
                            sb.append("GRAPH <" + contexts[i].stringValue() + "> {?s ?p ?o .} ");
                        }else{
                            sb.append("GRAPH <"+DEFAULT_GRAPH_URI+"> {?s ?p ?o .}");
                        }                    }
                    sb.append("}");
                }else{
                    sb.append("?s ?p ?o }");
                }
            }
            logger.debug(sb.toString());
            MarkLogicGraphQuery query = prepareGraphQuery(sb.toString());
            setBindings(query, subject, predicate, object, contexts);
            query.evaluate(handler);
        }
        catch (MalformedQueryException e) {
            throw new RepositoryException(e);
        }
        catch (QueryEvaluationException e) {
            throw new RepositoryException(e);
        }
    }

    /**
     * returns number of triples in the entire triple store
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
     * returns number of triples in supplied context
     *
     * @param contexts
     * @return long
     * @throws RepositoryException
     */
    @Override
    public long size(Resource... contexts) throws RepositoryException {
        if (contexts == null) {
            contexts = new Resource[] { null };
        }
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT (count(?s) as ?ct) where { GRAPH ?g { ?s ?p ?o }");
            boolean first = true;
            // with no args, measure the whole triple store.
            if (contexts != null && contexts.length > 0) {
                sb.append("filter (?g = (");
                for (Resource context : contexts) {
                    if (first) {
                        first = !first;
                    }
                    else {
                        sb.append(",");
                    }
                    if (context == null) {
                        sb.append("IRI(\""+DEFAULT_GRAPH_URI+"\")");
                    } else {
                        sb.append("IRI(\"" + context.toString() + "\")");
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
     * clears all triples from repository
     *
     * @throws RepositoryException
     */
    @Override
    public void clear() throws RepositoryException{
        getClient().sendClearAll();
    }

    /**
     * clears triples in supplied context
     *
     * @param contexts
     * @throws RepositoryException
     */
    @Override
    public void clear(Resource... contexts) throws RepositoryException {
        getClient().sendClear(contexts);
    }

    /**
     * returns true or false if the repository is empty or not
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
     * returns true if a transaction is active on connection
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
     * gets transaction isolationlevel (only IsolationLevels.SNAPSHOT supported)
     *
     * @return level
     */
    @Override
    public IsolationLevel getIsolationLevel() {
        return IsolationLevels.SNAPSHOT;
    }

    /**
     * sets transaction isolationlevel (only IsolationLevels.SNAPSHOT supported)
     *
     * @param level
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
     * opens a new transaction
     *
     * @throws RepositoryException
     */
    @Override
    public void begin() throws RepositoryException {
        getClient().openTransaction();
    }

    /**
     * opens a new transaction
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
     * commits transaction
     *
     * @throws RepositoryException
     */
    @Override
    public void commit() throws RepositoryException {
            getClient().commitTransaction();
    }

    /**
     * rollbacks open transaction
     *
     * @throws RepositoryException
     */
    @Override
    public void rollback() throws RepositoryException {
        getClient().rollbackTransaction();
    }

    /**
     * add triples via inputstream
     *
     * @param in
     * @param baseURI
     * @param dataFormat
     * @param contexts
     * @throws IOException
     * @throws RDFParseException
     * @throws RepositoryException
     */
    @Override
    public void add(InputStream in, String baseURI, RDFFormat dataFormat, Resource... contexts) throws IOException, RDFParseException, RepositoryException {
        getClient().sendAdd(in, baseURI, dataFormat, contexts);
    }

    /**
     * add triples via File
     *
     * will use file uri as base IRI if none supplied
     *
     * @param file
     * @param baseURI
     * @param dataFormat
     * @param contexts
     * @throws IOException
     * @throws RDFParseException
     * @throws RepositoryException
     */
    @Override
    public void add(File file, String baseURI, RDFFormat dataFormat, Resource... contexts) throws IOException, RDFParseException, RepositoryException {
        if(notNull(baseURI)) {
            getClient().sendAdd(file, baseURI, dataFormat, contexts);
        }else{
            getClient().sendAdd(file, file.toURI().toString(), dataFormat, contexts);
        }
    }

    /**
     * add triples via Reader
     *
     * @param reader
     * @param baseURI
     * @param dataFormat
     * @param contexts
     * @throws IOException
     * @throws RDFParseException
     * @throws RepositoryException
     */
    @Override
    public void add(Reader reader, String baseURI, RDFFormat dataFormat, Resource... contexts) throws IOException, RDFParseException, RepositoryException {
        getClient().sendAdd(reader, baseURI, dataFormat, contexts);
    }

    /**
     * add triples via URL
     *
     * sets base IRI to url if none is supplied
     *
     * @param url
     * @param baseURI
     * @param dataFormat
     * @param contexts
     * @throws IOException
     * @throws RDFParseException
     * @throws RepositoryException
     */
    @Override
    public void add(URL url, String baseURI, RDFFormat dataFormat, Resource... contexts) throws IOException, RDFParseException, RepositoryException {
        if(notNull(baseURI)) {
            getClient().sendAdd(new URL(url.toString()).openStream(), baseURI, dataFormat, contexts);
        }else{
            getClient().sendAdd(new URL(url.toString()).openStream(), url.toString(), dataFormat, contexts);
        }
    }

    /**
     * add single triple statement with supplied context
     *
     * @param subject
     * @param predicate
     * @param object
     * @param contexts
     * @throws RepositoryException
     */
    @Override
    public void add(Resource subject, IRI predicate, Value object, Resource... contexts) throws RepositoryException {
        getClient().sendAdd(null, subject, predicate, object, contexts);
    }

    /**
     * add single triple statement with supplied context
     *
     * @param st
     * @param contexts
     * @throws RepositoryException
     */
    @Override
    public void add(Statement st, Resource... contexts) throws RepositoryException {
        add(st.getSubject(), st.getPredicate(), st.getObject(), mergeResource(st.getContext(), contexts));
    }

    /**
     * add triple statements
     *
     * @param statements
     * @param contexts
     * @throws RepositoryException
     */
    @Override
    public void add(Iterable<? extends Statement> statements, Resource... contexts) throws RepositoryException {
        Iterator <? extends Statement> iter = statements.iterator();
        while(iter.hasNext()){
            Statement st = iter.next();
            add(st, mergeResource(st.getContext(), contexts));
        }
    }

    /**
     * add triple statements
     *
     * @param statements
     * @param contexts
     * @param <E>
     * @throws RepositoryException
     * @throws E
     */
    @Override
    public <E extends Exception> void add(Iteration<? extends Statement, E> statements, Resource... contexts) throws RepositoryException, E {
        while(statements.hasNext()){
            Statement st = statements.next();
            add(st.getSubject(), st.getPredicate(), st.getObject(), mergeResource(st.getContext(), contexts));
        }
    }


    /**
     * remove triple statement
     *
     * @param subject
     * @param predicate
     * @param object
     * @param contexts
     * @throws RepositoryException
     */
    @Override
    public void remove(Resource subject, IRI predicate, Value object, Resource... contexts) throws RepositoryException {
        getClient().sendRemove(null, subject, predicate, object, contexts);
    }

    /**
     * remove triple statement
     *
     * @param st
     * @param contexts
     * @throws RepositoryException
     */
    @Override
    public void remove(Statement st, Resource... contexts) throws RepositoryException {
        getClient().sendRemove(null, st.getSubject(), st.getPredicate(), st.getObject(), mergeResource(st.getContext(), contexts));
    }

    /**
     * remove triple statements
     *
     * @param statements
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
     * remove triple statements
     *
     * @param statements
     * @param contexts
     * @throws RepositoryException
     */
    @Override
    public void remove(Iterable<? extends Statement> statements, Resource... contexts) throws RepositoryException {
        Iterator <? extends Statement> iter = statements.iterator();
        while(iter.hasNext()){
            Statement st = iter.next();
            getClient().sendRemove(null, st.getSubject(), st.getPredicate(), st.getObject(), mergeResource(st.getContext(), contexts));
        }
    }

    /**
     * remove triple statements
     *
     * @param statements
     * @param <E>
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
     * remove triple statements
     *
     * @param statements
     * @param contexts
     * @param <E>
     * @throws RepositoryException
     * @throws E
     */
    @Override
    public <E extends Exception> void remove(Iteration<? extends Statement, E> statements, Resource... contexts) throws RepositoryException, E {
        while(statements.hasNext()){
            Statement st = statements.next();
            getClient().sendRemove(null, st.getSubject(), st.getPredicate(), st.getObject(), mergeResource(st.getContext(), contexts));
        }
    }

    /**
     * add without commit
     *
     * note- supplied to honor interface
     *
     * @param subject
     * @param predicate
     * @param object
     * @param contexts
     * @throws RepositoryException
     */
    @Override
    protected void addWithoutCommit(Resource subject, IRI predicate, Value object, Resource... contexts) throws RepositoryException {
        add(subject, predicate, object, contexts);
    }

    /**
     * remove without commit
     *
     *  supplied to honor interface
     *
     * @param subject
     * @param predicate
     * @param object
     * @param contexts
     * @throws RepositoryException
     */
    @Override
    protected void removeWithoutCommit(Resource subject, IRI predicate, Value object, Resource... contexts) throws RepositoryException {
        remove(subject, predicate, object, contexts);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // not in scope for 1.0.0 /////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /**
     *
     * supplied to honor interface
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
     * supplied to honor interface
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
     * supplied to honor interface
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
     * supplied to honor interface
     *
     * @param prefix
     * @throws RepositoryException
     */
    @Override
    public void removeNamespace(String prefix) throws RepositoryException {
    }

    /**
     *
     * supplied to honor interface
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
     * @param graphPerms
     */
    @Override
    public void setDefaultGraphPerms(GraphPermissions graphPerms) {
        if(notNull(graphPerms)) {
            this.defaultGraphPerms = graphPerms;
        }else{
            this.defaultGraphPerms = client.emptyGraphPerms();
        }
    }

    /**
     * returns default graph permissions to be used by all queries
     *
     * @return GraphPermissions
     */
    @Override
    public GraphPermissions getDefaultGraphPerms() {
        return this.defaultGraphPerms;
    }


    /**
     * sets default QueryDefinition to be used by all queries
     *
     * @param queryDef
     */
    @Override
    public void setDefaultConstrainingQueryDefinition(QueryDefinition queryDef) {
        this.defaultQueryDef = queryDef;
        //this.client.setConstrainingQueryDefinition(queryDef);
    }

    /**
     * returns default QueryDefinition to be used by all queries
     *
     * @return QueryDefinition
     */
    @Override
    public QueryDefinition getDefaultConstrainingQueryDefinition() {
        return this.defaultQueryDef;
    }

    /**
     * sets default rulesets to be used by all queries
     *
     * @param ruleset
     */
    @Override
    public void setDefaultRulesets(SPARQLRuleset ... ruleset ) {
        this.defaultRulesets = ruleset;
        //this.client.setRulesets(ruleset);
    }

    /**
     * returns default rulesets to be used by all queries
     *
     * @return SPARQLRuleset[]
     */
    @Override
    public SPARQLRuleset[] getDefaultRulesets() {
        return this.defaultRulesets;
    }


    /**
     * forces write cache to sync
     *
     */
    @Override
    public void sync() throws MarkLogicRdf4jException {
        client.sync();
    }

    /**
     * customise write cache interval and cache size. 
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
        if (contexts != null && contexts.length > 0) {
            SimpleDataset dataset = new SimpleDataset();
            if(notNull(contexts)){
                for (Resource context : contexts) {
                    if (notNull(context) || context instanceof IRI) {
                        dataset.addDefaultGraph((IRI) context);
                    } else {
                        dataset.addDefaultGraph(getValueFactory().createIRI(DEFAULT_GRAPH_URI));
                    }
                }
            }else{
                dataset.addDefaultGraph(getValueFactory().createIRI(DEFAULT_GRAPH_URI));
            }
            query.setDataset(dataset);
        }
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
    private static Resource[] mergeResource(Resource o, Resource... arr) {
        if(arr != null && arr.length > 0){
            return arr;
        } else if(o == null) {
            return null;
        } else {
            Resource[] newArray = new Resource[1];
            newArray[0] = o;
            return newArray;
        }

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
                    ctx = null;
                }
                return getValueFactory().createStatement(s, p, o, ctx);
            }
        };
    }

    /**
     * private utility method that tests if an object is null
     *
     * TBD -
     * @param item
     * @return boolean
     */
    private static Boolean notNull(Object item) {
        return item!=null;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////

}
