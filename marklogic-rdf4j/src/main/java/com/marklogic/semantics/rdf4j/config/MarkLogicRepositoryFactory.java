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
package com.marklogic.semantics.rdf4j.config;

import com.marklogic.semantics.rdf4j.MarkLogicRepository;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryFactory;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * A factory for instantiating MarkLogicRepository objects.
 *
 *
 */
public class MarkLogicRepositoryFactory implements RepositoryFactory {

	private static final Logger logger = LoggerFactory.getLogger(MarkLogicRepositoryFactory.class);

	public static final String REPOSITORY_TYPE = "marklogic:MarkLogicRepository";

    @Override
    /**
     * getter for repository type.
     *
     */
    public String getRepositoryType() {
		return REPOSITORY_TYPE;
	}

    @Override
    /**
     * returns config
     *
     */
    public MarkLogicRepositoryConfig getConfig() {
        return new MarkLogicRepositoryConfig();
	}

    @Override
    /**
     * Instantiate and return repository object.
     *
     */
    public MarkLogicRepository getRepository(RepositoryImplConfig config) throws RepositoryConfigException {
        MarkLogicRepository repo = null;
        MarkLogicRepositoryConfig cfg = (MarkLogicRepositoryConfig) config;
        if (cfg.getHost() != null && cfg.getPort() != 0) {
            // init with MarkLogicRepositoryConfig
            repo = new MarkLogicRepository(cfg.getHost(),cfg.getPort(),cfg.getUser(),cfg.getPassword(),cfg.getAuth());
        } else if (cfg.getHost() == null) {
            // init with queryEndpoint as connection string
            try {
                repo = new MarkLogicRepository(new URL(cfg.getQueryEndpointUrl()));
            } catch (MalformedURLException e) {
                logger.debug(e.getMessage());
                throw new RepositoryConfigException(e.getMessage());
            }
        }else{
            throw new RepositoryConfigException("Invalid configuration class: " + config.getClass());
        }
        return repo;
    }
}