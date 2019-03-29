/*
 * Copyright 2015-2019 MarkLogic Corporation
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
package com.marklogic.semantics.rdf4j.config;

import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.repository.config.AbstractRepositoryImplConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Representation of Repository config
 *
 *
 */
public class MarkLogicRepositoryConfig extends AbstractRepositoryImplConfig {

	private static final Logger logger = LoggerFactory.getLogger(MarkLogicRepositoryConfig.class);

	public static final ValueFactory vf= SimpleValueFactory.getInstance();

    public static final IRI QUERY_ENDPOINT = vf.createIRI(
            "http://www.marklogic.com/v1/graphs/sparql");

    public static final IRI UPDATE_ENDPOINT = vf.createIRI(
            "http://www.marklogic.com/v1/graphs");

	private String queryEndpointUrl;
	private String updateEndpointUrl;

	private String host;
	private int port;
	private String user;
	private String password;
	private String auth;

    /**
	 * Base constructor
	 */
	public MarkLogicRepositoryConfig() {
		super(MarkLogicRepositoryFactory.REPOSITORY_TYPE);
	}

    /**
	 * Constructor initialized with all connection details.
	 */
	public MarkLogicRepositoryConfig(String host, int port, String user, String password, String auth) {
        this();
        setHost(host);
        setPort(port);
        setUser(user);
        setPassword(password);
        setAuth(auth);
        setQueryEndpointUrl("http://" + user + ":" + password + "@" + host + ":" + port + "/v1/graphs/sparql");
        setUpdateEndpointUrl("http://" + user + ":" + password + "@" + host + ":" + port + "/v1/graphs");
    }

	/**
	 * Constructor initialized with query endpoint
	 *
	 * @param queryEndpointUrl
	 */
	public MarkLogicRepositoryConfig(String queryEndpointUrl) {
        setQueryEndpointUrl(queryEndpointUrl);
	}

	/**
	 * Constructor initialized with both query and update endpoint
	 *
	 * @param queryEndpointUrl
	 * @param updateEndpointUrl
	 */
	public MarkLogicRepositoryConfig(String queryEndpointUrl, String updateEndpointUrl) {
		this(queryEndpointUrl);
		setUpdateEndpointUrl(updateEndpointUrl);
	}

	/**
	 * MarkLogicRepositoryConfig specific getter/setter for host
	 *
	 * @return
	 */
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * MarkLogicRepositoryConfig specific getter/setter for port
	 *
	 * @return
	 */
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * MarkLogicRepositoryConfig specific getter/setter for user
	 *
	 */
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}

	/**
	 * MarkLogicRepositoryConfig specific getter/setter for password
	 *
	 */
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * MarkLogicRepositoryConfig specific getter/setter for auth
	 *
	 */
	public String getAuth() {
		return auth;
	}
	public void setAuth(String auth) {
		this.auth = auth;
	}

	/**
	 * MarkLogicRepositoryConfig specific getter/setter for connection string
	 *
	 */
	public String getQueryEndpointUrl() {
		return queryEndpointUrl;
	}
	public void setQueryEndpointUrl(String url) {
		this.queryEndpointUrl = url;
	}

	/**
	 * MarkLogicRepositoryConfig specific getter/setter for connection string
	 *
	 */
	public String getUpdateEndpointUrl() {
		return updateEndpointUrl;
	}
	public void setUpdateEndpointUrl(String url) {
		this.updateEndpointUrl = url;
	}
	
	@Override
	/**
	 * Validate configuration.
	 *
	 */
	public void validate() throws RepositoryConfigException {
		super.validate();
		if (getQueryEndpointUrl() == null) {
			throw new RepositoryConfigException(
					"No endpoint URL specified for SPARQL repository");
		}
	}

	@Override
	/**
	 * Export model representation of config.
	 *
	 */
	public Resource export(Model model) {
		Resource implNode = super.export(model);
		ValueFactory vf = SimpleValueFactory.getInstance();
		if (getQueryEndpointUrl() != null) {
			model.add(implNode, QUERY_ENDPOINT, vf.createIRI(getQueryEndpointUrl()));
		}
		if (getUpdateEndpointUrl() != null) {
			model.add(implNode, UPDATE_ENDPOINT, vf.createIRI(getUpdateEndpointUrl()));
		}

		return implNode;
	}

	@Override
	/**
	 * Parse graph representation of config.
	 *
	 */
	public void parse(Model model, Resource implNode)
			throws RepositoryConfigException {
		super.parse(model, implNode);

		try {
			IRI iri = Models.getPropertyIRI(model, implNode, QUERY_ENDPOINT).orElse(null);
			if (iri != null) {
				setQueryEndpointUrl(iri.stringValue());
			}
			iri = Models.getPropertyIRI(model, implNode, UPDATE_ENDPOINT).orElse(null);
			if (iri != null) {
				setUpdateEndpointUrl(iri.stringValue());
			}
		} catch (RDF4JException e) {
			throw new RepositoryConfigException(e.getMessage(), e);
		}
	}
}