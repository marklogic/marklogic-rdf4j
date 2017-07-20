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
package com.marklogic.semantics.rdf4j.query;

import com.marklogic.client.FailedRequestException;
import com.marklogic.client.ForbiddenUserException;
import com.marklogic.client.semantics.GraphPermissions;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.semantics.SPARQLRuleset;
import com.marklogic.semantics.rdf4j.client.MarkLogicClient;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sparql.query.SPARQLQueryBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * update query
 *
 * @author James Fuller
 */
public class MarkLogicUpdateQuery extends MarkLogicQuery implements Update,MarkLogicQueryDependent {

    private static final Logger logger = LoggerFactory.getLogger(MarkLogicUpdateQuery.class);

    /**
     *  constructor
     *
     * @param client
     * @param bindingSet
     * @param baseUri
     * @param queryString
     */
    public MarkLogicUpdateQuery(MarkLogicClient client, SPARQLQueryBindingSet bindingSet, String baseUri, String queryString, GraphPermissions graphPerms, QueryDefinition queryDef, SPARQLRuleset[] rulesets) {
        super(client, bindingSet, baseUri, queryString,graphPerms,queryDef,rulesets);
    }

    /**
     * execute update query
     *
     * @throws UpdateExecutionException
     */
    @Override
    public void execute() throws UpdateExecutionException {
        try {
            sync();
            getMarkLogicClient().sendUpdateQuery(getQueryString(), getBindings(), getIncludeInferred(), getBaseURI());
        }catch(ForbiddenUserException | FailedRequestException e){
            throw new UpdateExecutionException(e);
        } catch (RepositoryException e) {
            throw new UpdateExecutionException(e);
        } catch (MalformedQueryException e) {
            throw new UpdateExecutionException(e);
        } catch (IOException e) {
            throw new UpdateExecutionException(e);
        }
    }

}
