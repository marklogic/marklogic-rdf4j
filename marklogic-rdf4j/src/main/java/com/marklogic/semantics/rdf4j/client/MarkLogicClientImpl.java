/*
 * Copyright 2015-2018 MarkLogic Corporation
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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.marklogic.semantics.rdf4j.utils.Util;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sparql.query.SPARQLQueryBindingSet;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.FailedRequestException;
import com.marklogic.client.ForbiddenUserException;
import com.marklogic.client.Transaction;
import com.marklogic.client.impl.SPARQLBindingsImpl;
import com.marklogic.client.io.FileHandle;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.GraphPermissions;
import com.marklogic.client.semantics.RDFTypes;
import com.marklogic.client.semantics.SPARQLBindings;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.client.semantics.SPARQLQueryManager;
import com.marklogic.client.semantics.SPARQLRuleset;
import com.marklogic.semantics.rdf4j.MarkLogicRdf4jException;

/**
 * Internal class for interacting with Java Client API.
 *
 *
 */
public class MarkLogicClientImpl {

    private static final Logger logger = LoggerFactory.getLogger(MarkLogicClientImpl.class);

    private static final String DEFAULT_GRAPH_URI = "http://marklogic.com/semantics#default-graph";

    private SPARQLRuleset[] ruleset;
    private QueryDefinition constrainingQueryDef;
    private GraphPermissions graphPerms;

    private SPARQLQueryManager sparqlManager;
    private GraphManager graphManager;

    private DatabaseClient databaseClient;

    private Util util = Util.getInstance();

    /**
     * Constructor initialized with connection parameters.
     *
     * @param host
     * @param port
     * @param user
     * @param password
     * @param auth
     */
    public MarkLogicClientImpl(String host, int port, String user, String password, String database, String auth) {
        setDatabaseClient(util.getClientBasedOnAuth(host, port, user, password, database, auth));
    }

    /**
     *  set databaseclient
     *
     * @param databaseClient
     */
    public MarkLogicClientImpl(DatabaseClient databaseClient) {
        setDatabaseClient(databaseClient);
    }

    /**
     * set databaseclient and instantiate related managers.
     *
     * @param databaseClient
     */
    private void setDatabaseClient(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
        this.sparqlManager = getDatabaseClient().newSPARQLQueryManager();
        this.graphManager = getDatabaseClient().newGraphManager();
    }

    /**
     * gets database client
     *
     * @return DatabaseClient
     */
    public DatabaseClient getDatabaseClient() {
        return this.databaseClient;
    }

    /**
     * Executes SPARQLQuery
     *
     * @param queryString
     * @param bindings
     * @param start
     * @param pageLength
     * @param tx
     * @param includeInferred
     * @param baseURI
     * @return
     * @throws JsonProcessingException
     */
    public InputStream performSPARQLQuery(String queryString, SPARQLQueryBindingSet bindings, long start, long pageLength, Transaction tx, boolean includeInferred, String baseURI) throws JsonProcessingException {
        return performSPARQLQuery(queryString, bindings, new InputStreamHandle(), start, pageLength, tx, includeInferred, baseURI);
    }

    /**
     * Executes SPARQLQuery with InputStreamHandle
     *
     * @param queryString
     * @param bindings
     * @param handle
     * @param start
     * @param pageLength
     * @param tx
     * @param includeInferred
     * @param baseURI
     * @return
     * @throws JsonProcessingException
     */
    public InputStream performSPARQLQuery(String queryString, SPARQLQueryBindingSet bindings, InputStreamHandle handle, long start, long pageLength, Transaction tx, boolean includeInferred, String baseURI) throws JsonProcessingException {
        SPARQLQueryDefinition qdef = sparqlManager.newQueryDefinition(queryString);
        if(Util.notNull(baseURI) && !baseURI.isEmpty()){ qdef.setBaseUri(baseURI);}
        if (Util.notNull(ruleset) && includeInferred){qdef.setRulesets(ruleset);}
        if (Util.notNull(getConstrainingQueryDefinition())) {
        	qdef.setConstrainingQueryDefinition(getConstrainingQueryDefinition());
            qdef.setDirectory(getConstrainingQueryDefinition().getDirectory());
            qdef.setCollections(getConstrainingQueryDefinition().getCollections());
            qdef.setResponseTransform(getConstrainingQueryDefinition().getResponseTransform());
            qdef.setOptionsName(getConstrainingQueryDefinition().getOptionsName());
        }
        qdef.setIncludeDefaultRulesets(includeInferred);
        if(Util.notNull(graphPerms)){ qdef.setUpdatePermissions(graphPerms);}
        if(pageLength > 0){
            sparqlManager.setPageLength(pageLength);
        }else{
            sparqlManager.clearPageLength();
        }
        sparqlManager.executeSelect(qdef, handle, start, tx);
        return new BufferedInputStream(handle.get());
    }

    /**
     * Executes GraphQuery
     * @param queryString
     * @param bindings
     * @param tx
     * @param includeInferred
     * @param baseURI
     * @return
     * @throws JsonProcessingException
     */
    public InputStream performGraphQuery(String queryString, SPARQLQueryBindingSet bindings, Transaction tx, boolean includeInferred, String baseURI) throws JsonProcessingException {
        return performGraphQuery(queryString, bindings, new InputStreamHandle(), tx, includeInferred, baseURI);
    }

    /**
     * Executes GraphQuery with InputStreamHandle.
     *
     * @param queryString
     * @param bindings
     * @param handle
     * @param tx
     * @param includeInferred
     * @param baseURI
     * @return
     * @throws JsonProcessingException
     */
    public InputStream performGraphQuery(String queryString, SPARQLQueryBindingSet bindings, InputStreamHandle handle, Transaction tx, boolean includeInferred, String baseURI) throws JsonProcessingException  {
        SPARQLQueryDefinition qdef = sparqlManager.newQueryDefinition(queryString);
        if(Util.notNull(baseURI) && !baseURI.isEmpty()){ qdef.setBaseUri(baseURI);}
        if (Util.notNull(ruleset) && includeInferred) {qdef.setRulesets(ruleset);}
        if (Util.notNull(getConstrainingQueryDefinition())){
        	qdef.setConstrainingQueryDefinition(getConstrainingQueryDefinition());
            qdef.setDirectory(getConstrainingQueryDefinition().getDirectory());
            qdef.setCollections(getConstrainingQueryDefinition().getCollections());
            qdef.setResponseTransform(getConstrainingQueryDefinition().getResponseTransform());
            qdef.setOptionsName(getConstrainingQueryDefinition().getOptionsName());
        	}
        if(Util.notNull(graphPerms)){ qdef.setUpdatePermissions(graphPerms);}
        qdef.setIncludeDefaultRulesets(includeInferred);
        sparqlManager.executeDescribe(qdef, handle, tx);
        return new BufferedInputStream(handle.get());
    }

    /**
     * Executes BooleanQuery
     *
     * @param queryString
     * @param bindings
     * @param tx
     * @param includeInferred
     * @param baseURI
     * @return
     */
    public boolean performBooleanQuery(String queryString, SPARQLQueryBindingSet bindings, Transaction tx, boolean includeInferred, String baseURI) {
        SPARQLQueryDefinition qdef = sparqlManager.newQueryDefinition(queryString);
        if(Util.notNull(baseURI) && !baseURI.isEmpty()){ qdef.setBaseUri(baseURI);}
        qdef.setIncludeDefaultRulesets(includeInferred);
        if (Util.notNull(ruleset) && includeInferred) {qdef.setRulesets(ruleset);}
        if (Util.notNull(getConstrainingQueryDefinition())){
        	qdef.setConstrainingQueryDefinition(getConstrainingQueryDefinition());
            qdef.setDirectory(getConstrainingQueryDefinition().getDirectory());
            qdef.setCollections(getConstrainingQueryDefinition().getCollections());
            qdef.setResponseTransform(getConstrainingQueryDefinition().getResponseTransform());
            qdef.setOptionsName(getConstrainingQueryDefinition().getOptionsName());
        	}
        if(Util.notNull(graphPerms)){ qdef.setUpdatePermissions(graphPerms);}
        return sparqlManager.executeAsk(qdef,tx);
    }

    /**
     * Executes UpdateQuery
     *
     * @param queryString
     * @param bindings
     * @param tx
     * @param includeInferred
     * @param baseURI
     */
    public void performUpdateQuery(String queryString, SPARQLQueryBindingSet bindings, Transaction tx, boolean includeInferred, String baseURI) {
        SPARQLQueryDefinition qdef = sparqlManager.newQueryDefinition(queryString);
        if(Util.notNull(baseURI) && !baseURI.isEmpty()){ qdef.setBaseUri(baseURI);}
        if (Util.notNull(ruleset) && includeInferred) {qdef.setRulesets(ruleset);}
        if(Util.notNull(graphPerms)){ qdef.setUpdatePermissions(graphPerms);}
        qdef.setIncludeDefaultRulesets(includeInferred);
        sparqlManager.clearPageLength();
        try {
            sparqlManager.executeUpdate(qdef, tx);
        }
        catch (ForbiddenUserException e)
        {
            throw new RepositoryException(e.getMessage());
        }
    }

    /**
     * Executes merge of triples from File
     *
     * @param file
     * @param baseURI
     * @param dataFormat
     * @param tx
     * @param contexts
     * @throws RDFParseException
     */
    // performAdd
    // as we use mergeGraphs, baseURI is always file.toURI
    public void performAdd(File file, String baseURI, RDFFormat dataFormat, Transaction tx, Resource... contexts) throws RDFParseException {
        try {
            graphManager.setDefaultMimetype(dataFormat.getDefaultMIMEType());
            if (dataFormat.equals(RDFFormat.NQUADS) || dataFormat.equals(RDFFormat.TRIG)) {
                graphManager.mergeGraphs(new FileHandle(file),tx);
            } else {
                if (contexts.length>0) {
                    for (int i = 0; i < contexts.length; i++) {
                        if(Util.notNull(contexts[i])){
                            graphManager.mergeAs(contexts[i].toString(), new FileHandle(file), getGraphPerms(),tx);
                        }else{
                            graphManager.mergeAs(DEFAULT_GRAPH_URI, new FileHandle(file), getGraphPerms(), tx);
                        }
                    }
                } else {
                    graphManager.mergeAs(DEFAULT_GRAPH_URI, new FileHandle(file), getGraphPerms(),tx);
                }
            }
        } catch (FailedRequestException e) {
            logger.error(e.getLocalizedMessage());
            throw new RDFParseException("Request to MarkLogic server failed, check file and format.");
        }
    }

    /**
     * Executes merge of triples from InputStream.
     *
     * @param in
     * @param baseURI
     * @param dataFormat
     * @param tx
     * @param contexts
     * @throws RDFParseException
     */
    public void performAdd(InputStream in, String baseURI, RDFFormat dataFormat, Transaction tx, Resource... contexts) throws RDFParseException, MarkLogicRdf4jException {
        try {
            graphManager.setDefaultMimetype(dataFormat.getDefaultMIMEType());
            if (dataFormat.equals(RDFFormat.NQUADS) || dataFormat.equals(RDFFormat.TRIG)) {
                graphManager.mergeGraphs(new InputStreamHandle(in),tx);
            } else {
                if (contexts.length > 0) {
                    for (int i = 0; i < contexts.length; i++) {
                        if (Util.notNull(contexts[i])) {
                            graphManager.mergeAs(contexts[i].toString(), new InputStreamHandle(in), getGraphPerms(), tx);
                        } else {
                            graphManager.mergeAs(DEFAULT_GRAPH_URI, new InputStreamHandle(in),getGraphPerms(), tx);
                        }
                    }
                } else {
                    graphManager.mergeAs(DEFAULT_GRAPH_URI, new InputStreamHandle(in),getGraphPerms(), tx);
                }
            }
            in.close();
        } catch (FailedRequestException e) {
            logger.error(e.getLocalizedMessage());
            throw new RDFParseException("Request to MarkLogic server failed, check input is valid.");
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage());
            throw new MarkLogicRdf4jException("IO error");
        }
    }

    /**
     * Executes INSERT of single triple.
     *
     * @param baseURI
     * @param subject
     * @param predicate
     * @param object
     * @param tx
     * @param contexts
     * @throws MarkLogicRdf4jException
     */
    public void performAdd(String baseURI, Resource subject, IRI predicate, Value object, Transaction tx, Resource... contexts) throws MarkLogicRdf4jException {
        StringBuilder sb = new StringBuilder();
        if(contexts.length>0) {
            if (Util.notNull(baseURI)) sb.append("BASE <" + baseURI + ">\n");
            sb.append("INSERT DATA { ");
            for (int i = 0; i < contexts.length; i++) {
                if (Util.notNull(contexts[i])) {
                    sb.append("GRAPH <" + contexts[i].stringValue() + "> { ?s ?p ?o .} ");
                } else {
                    sb.append("GRAPH <" + DEFAULT_GRAPH_URI + "> { ?s ?p ?o .} ");
                }
            }
            sb.append("}");
        } else {
            sb.append("INSERT DATA { GRAPH <" + DEFAULT_GRAPH_URI + "> {?s ?p ?o .}}");
        }  
        SPARQLQueryDefinition qdef = sparqlManager.newQueryDefinition(sb.toString());
        if (Util.notNull(ruleset) ) {qdef.setRulesets(ruleset);}
        if(Util.notNull(graphPerms)){ qdef.setUpdatePermissions(graphPerms);}
        if(Util.notNull(baseURI) && !baseURI.isEmpty()){ qdef.setBaseUri(baseURI);}

        if(Util.notNull(subject)) qdef.withBinding("s", subject.stringValue());
        if(Util.notNull(predicate)) qdef.withBinding("p", predicate.stringValue());
        if(Util.notNull(object)) bindObject(qdef, "o", object);
        sparqlManager.executeUpdate(qdef, tx);
    }

    /**
     * Executes DELETE of single triple.
     *
     * @param baseURI
     * @param subject
     * @param predicate
     * @param object
     * @param tx
     * @param contexts
     * @throws MarkLogicRdf4jException
     */
    public void performRemove(String baseURI, Resource subject, IRI predicate, Value object, Transaction tx, Resource... contexts) throws MarkLogicRdf4jException {
        StringBuilder sb = new StringBuilder();
        String[] contextArgs = null;
        if(contexts.length>0)
        {	if (Util.notNull(baseURI))sb.append("BASE <" + baseURI + ">\n");
            contextArgs = new String[contexts.length];
            for (int i = 0; i < contexts.length; i++) {
                if(Util.notNull(contexts[i])){
                    contextArgs[i] = contexts[i].stringValue();
                }
                else{
                	contextArgs[i] = DEFAULT_GRAPH_URI;
                }
            }
        }
        sb.append("DELETE WHERE { GRAPH ?ctx { ?s ?p ?o .}}");
        SPARQLQueryDefinition qdef = sparqlManager.newQueryDefinition(sb.toString());
        if(Util.notNull(contextArgs)) qdef.setUsingNamedGraphUris(contextArgs);
        if(Util.notNull(baseURI) && !baseURI.isEmpty()){ qdef.setBaseUri(baseURI);}
        if(Util.notNull(subject)) qdef.withBinding("s", subject.stringValue());
        if(Util.notNull(predicate)) qdef.withBinding("p", predicate.stringValue());
        if(Util.notNull(object)) bindObject(qdef, "o", object);
        sparqlManager.executeUpdate(qdef, tx);
    }

    /**
     * Clears triples from named graph.
     *
     * @param tx
     * @param contexts
     */
    public void performClear(Transaction tx, Resource... contexts) {
        if(contexts.length>0) {
            for (int i = 0; i < contexts.length; i++) {
                if (Util.notNull(contexts[i])) {
                    graphManager.delete(contexts[i].stringValue(), tx);
                } else {
                    graphManager.delete(DEFAULT_GRAPH_URI, tx);
                }
            }
        }else{
            graphManager.delete(DEFAULT_GRAPH_URI, tx);
        }
    }

    /**
     * Clears all triples.
     *
     * @param tx
     */
    public void performClearAll(Transaction tx) {
        graphManager.deleteGraphs(tx);
    }

    /**
     * getter rulesets
     *
     * @return
     */
    public SPARQLRuleset[] getRulesets() {
        return this.ruleset;
    }

    /**
     * setter for rulesets, filters out nulls
     *
     * @param rulesets
     */
    public void setRulesets(SPARQLRuleset ... rulesets) {
        if(Util.notNull(rulesets)) {
            List<SPARQLRuleset> list = new ArrayList<>();
            for(Object r : rulesets) {
                if(r != null && rulesets.length > 0) {
                    list.add((SPARQLRuleset)r);
                }
            }
            this.ruleset = list.toArray(new SPARQLRuleset[list.size()]);
        }else{
            this.ruleset = null;
        }
    }

    /**
     * setter for graph permissions
     *
     * @param graphPerms
     */
    public void setGraphPerms(GraphPermissions graphPerms) {
        this.graphPerms = graphPerms;
    }

    /**
     * getter for graph permissions
     *
     * @return
     */
    public GraphPermissions getGraphPerms() {
        return this.graphPerms;
    }

    /**
     * setter for ConstrainingQueryDefinition
     *
     * @param constrainingQueryDefinition
     */
    public void setConstrainingQueryDefinition(QueryDefinition constrainingQueryDefinition) {
        this.constrainingQueryDef = constrainingQueryDefinition;
    }

    /**
     * getter for ConstrainingQueryDefinition
     *
     * @return
     */
    public QueryDefinition getConstrainingQueryDefinition() {
        return this.constrainingQueryDef;
    }

    /**
     * Close client.
     *
     * @return
     */
    public void close() {
        // close MarkLogicClientImpl
    }

    public void release() {
        if (this.databaseClient != null) {
            try {
                this.databaseClient.release();
            } catch (Exception e) {
                logger.info("Failed releasing DB client", e);
            }
        }
    }
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Converts RDF4J BindingSet to java client api SPARQLBindings
     *
     * @param bindings
     * @return
     */
    protected SPARQLBindings getSPARQLBindings(SPARQLQueryBindingSet bindings) {
        SPARQLBindings sps = new SPARQLBindingsImpl();
        for (Binding binding : bindings) {
            sps.bind(binding.getName(), binding.getValue().stringValue());
        }
        return sps;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Bind object.
     *
     * @param qdef
     * @param variableName
     * @param object
     * @return
     * @throws MarkLogicRdf4jException
     */
    private static SPARQLQueryDefinition bindObject(SPARQLQueryDefinition qdef, String variableName, Value object) throws MarkLogicRdf4jException {
        SPARQLBindings bindings = qdef.getBindings();
        if(object != null){
            if (object instanceof IRI) {
                bindings.bind(variableName, object.stringValue());
            } else if (object instanceof Literal) {
                Literal lit = (Literal) object;
                if (lit.getLanguage().orElse(null) != null) {
                    String languageTag = lit.getLanguage().orElse(null);
                    bindings.bind(variableName, lit.getLabel(), Locale.forLanguageTag(languageTag));
                }else if (((Literal) object).getDatatype() != null) {
                    try {
                        String xsdType = lit.getDatatype().toString();
                        String fragment = new java.net.URI(xsdType).getFragment();
                        bindings.bind(variableName,lit.getLabel(),RDFTypes.valueOf(fragment.toUpperCase()));
                    } catch (URISyntaxException e) {
                        logger.error(e.getLocalizedMessage());
                        throw new MarkLogicRdf4jException("Problem with object datatype.");
                    }
                }else {
                    // assume we have a string value
                    bindings.bind(variableName, lit.getLabel(), RDFTypes.STRING);
                }
            }
            qdef.setBindings(bindings);
        }
        return qdef;
    }
}