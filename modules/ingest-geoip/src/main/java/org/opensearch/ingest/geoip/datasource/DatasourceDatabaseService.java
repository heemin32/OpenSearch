/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.ingest.geoip.datasource.common.DatasourceManifest;
import org.opensearch.ingest.geoip.datasource.common.DatasourceMetadata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.opensearch.ingest.geoip.IngestGeoIpModulePlugin.GEOIP_DATASOURCE_INDEX_NAME;

/**
 * Set of methods to operate on datasource database index
 *
 * Datasource database index contains the GeoIP database for each datasource.
 */
public class DatasourceDatabaseService {
    private static final Logger LOGGER = LogManager.getLogger(DatasourceDatabaseService.class);
    public static final String IP_RANGE_FIELD_NAME = "_cidr";
    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public DatasourceDatabaseService(final Client client,
                                     final ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    public CSVParser getDatabaseReader(final DatasourceManifest manifest) throws IOException {
        URL zipUrl = new URL(manifest.getUrl());
        ZipInputStream zipIn = new ZipInputStream(zipUrl.openStream());
        ZipEntry zipEntry = zipIn.getNextEntry();
        while(zipEntry != null) {
            if (!zipEntry.getName().equalsIgnoreCase(manifest.getDbName())) {
                zipEntry = zipIn.getNextEntry();
                continue;
            }
            return new CSVParser(new BufferedReader(new InputStreamReader(zipIn)), CSVFormat.RFC4180);
        }
        LOGGER.error("ZIP file {} does not have database file {}", manifest.getUrl(), manifest.getDbName());
        throw new RuntimeException("ZIP file does not have database file");
    }

    /**
     * Create a document in json string format to ingest in datasource database index
     *
     * It assumes the first field as ip_range. The rest is added under data field.
     *
     * Document example
     * {
     *   "_cidr":"1.0.0.1/25",
     *   "data":{
     *       "country": "USA",
     *       "city": "Seattle",
     *       "location":"13.23,42.12"
     *   }
     * }
     *
     * @param fields a list of field name
     * @param values a list of values
     * @return Document in json string format
     */
    public String createDocument(final String[] fields, final String[] values) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"");
        sb.append(IP_RANGE_FIELD_NAME);
        sb.append("\":\"");
        sb.append(values[0]);
        sb.append("\",\"data\":{");
        for (int i = 1; i < fields.length; i++) {
            if (i != 1) {
                sb.append(",");
            }
            sb.append("\"");
            sb.append(fields[i]);
            sb.append("\":\"");
            sb.append(values[i]);
            sb.append("\"");
        }
        sb.append("}}");
        return sb.toString();
    }

    public void createNextIndex(final String indexName, final TimeValue timeout) throws IOException {
        if(clusterService.state().metadata().hasIndex(indexName) == true) {
            LOGGER.info("Index {} already exist. Skipping creation.", indexName);
            return;
        }
        final Map<String, Object> indexSettings = new HashMap<>();
        indexSettings.put("index.number_of_shards", 1);
        indexSettings.put("index.auto_expand_replicas", "0-all");
        final CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName)
            .settings(indexSettings)
            .mapping(getIndexMapping());
        CreateIndexResponse response = client.admin()
            .indices()
            .create(createIndexRequest)
            .actionGet(timeout);
        LOGGER.info("Index {} created?: {}", indexName, response.isAcknowledged());
    }

    /**
     * Generate XContentBuilder representing datasource database index mapping
     *
     * {
     *     "dynamic": false,
     *     "properties": {
     *         "_cidr": {
     *             "type": "ip_range",
     *             "doc_values": false
     *         }
     *     }
     * }
     *
     * @return XContentBuilder representing datasource database index mapping
     * @throws IOException
     */
    private XContentBuilder getIndexMapping() throws IOException {
        return XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .field("dynamic", false)
            .startObject("properties")
            .startObject(IP_RANGE_FIELD_NAME)
            .field("type", "ip_range")
            .field("doc_values", false)
            .endObject()
            .endObject()
            .endObject();
    }
}
