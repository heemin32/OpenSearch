/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.ingest.geoip.datasource.common.DatasourceMetadata;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.ingest.geoip.IngestGeoIpModulePlugin.GEOIP_DATASOURCE_INDEX_NAME;

/**
 * Set of methods to operate on datasource metadata index
 *
 * Datasource metadata index contains metadata of every datasource.
 */
public class DatasourceMetadataService {
    private static final Logger LOGGER = LogManager.getLogger(DatasourceMetadataService.class);
    private static final String INDEX_NAME = GEOIP_DATASOURCE_INDEX_NAME;
    private static final TimeValue SWEEP_SEARCH_TIMEOUT = TimeValue.timeValueMinutes(1);
    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public DatasourceMetadataService(final Client client,
                                     final ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    public void addDatasourceIfAbsent(final PutDatasourceRequest request, final ActionListener<DatasourceMetadata> listener) throws IOException {
        DatasourceMetadata metadata = DatasourceMetadata.Builder.build(request);

        IndexRequestBuilder requestBuilder = client.prepareIndex(INDEX_NAME);
        requestBuilder.setId(request.getId());
        requestBuilder.setOpType(DocWriteRequest.OpType.CREATE);
        requestBuilder.setSource(metadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));

        client.index(requestBuilder.request(), new ActionListener<>() {
            @Override
            public void onResponse(final IndexResponse indexResponse) {
                listener.onResponse(metadata);
            }

            @Override
            public void onFailure(final Exception e) {
                if (e instanceof VersionConflictEngineException) {
                    LOGGER.info("Datasource already exists {}", request.getId(), e);
                    listener.onFailure(new ResourceAlreadyExistsException("Datasource already exists"));
                } else {
                    LOGGER.error("Failed to create a datasource {}", request.getId(), e);
                    listener.onFailure(new OpenSearchException("Failed to create a datasource"));
                }
            }
        });
    }

    public void createDatasourceIndexIfAbsent() {
        try {
            final Map<String, Object> indexSettings = new HashMap<>();
            indexSettings.put("index.number_of_shards", 1);
            indexSettings.put("index.auto_expand_replicas", "0-all");
            final CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_NAME)
                .settings(indexSettings);
            CreateIndexResponse response = client.admin()
                .indices()
                .create(createIndexRequest)
                .actionGet();
            LOGGER.info("Index {} created?: {}", INDEX_NAME, response.isAcknowledged());
        } catch (ResourceAlreadyExistsException resourceAlreadyExistsException) {
            LOGGER.info("Index {} already exists", INDEX_NAME);
        }
    }

    public List<DatasourceMetadata> sweepDatasourceMetadata(final String startAfter, final int sweepPageMaxSize) {
        if (clusterService.state().metadata().hasIndex(INDEX_NAME) == false) {
            return Collections.emptyList();
        }

        String searchAfter = startAfter == null ? "" : startAfter;
        LOGGER.info("Sweep datasource metadata after id {}", startAfter);
        SearchRequest searchRequest = new SearchRequest().indices(INDEX_NAME)
            .source(
                new SearchSourceBuilder().version(true)
                    .seqNoAndPrimaryTerm(true)
                    .sort(new FieldSortBuilder("_id").unmappedType("keyword").missing("_last"))
                    .searchAfter(new String[] { searchAfter })
                    .size(sweepPageMaxSize)
                    .query(QueryBuilders.matchAllQuery())
            );

        SearchResponse response = client.search(searchRequest).actionGet(SWEEP_SEARCH_TIMEOUT);
        if (response.status() != RestStatus.OK) {
            LOGGER.error("Error sweeping datasource metadata: {}", response.status());
            return Collections.emptyList();
        }

        if (response.getHits() == null || response.getHits().getHits().length < 1) {
            return Collections.emptyList();
        }

        List<DatasourceMetadata> metadata = new ArrayList<>(response.getHits().getHits().length);
        for (SearchHit hit : response.getHits()) {
            try {
                metadata.add(toMetadata(hit.getId(), hit.getSourceRef()));
            } catch (IOException e) {
                LOGGER.error("Error parsing metadata of id {}", hit.getId(), e);
            }
        }
        return metadata;
    }

    public DatasourceMetadata getMetadata(final String id, final TimeValue timeout) throws IOException {
        GetRequest request = new GetRequest(INDEX_NAME, id);
        GetResponse response = client.get(request).actionGet(timeout);
        XContentParser parser = XContentHelper.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.getSourceAsBytesRef()
        );
        DatasourceMetadata metadata = new DatasourceMetadata(response.getId());
        DatasourceMetadata.PARSER.parse(parser, metadata, null);
        return metadata;
    }

    public void updateMetadata(final DatasourceMetadata metadata) throws IOException {
        IndexRequestBuilder requestBuilder = client.prepareIndex(INDEX_NAME);
        requestBuilder.setId(metadata.getId());
        requestBuilder.setOpType(DocWriteRequest.OpType.INDEX);
        requestBuilder.setSource(metadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
        client.index(requestBuilder.request());
    }

    private DatasourceMetadata toMetadata(final String id, final BytesReference source) throws IOException {
        XContentParser parser = XContentHelper.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            source
        );
        DatasourceMetadata metadata = new DatasourceMetadata(id);
        DatasourceMetadata.PARSER.parse(parser, metadata, null);
        return metadata;
    }
}
