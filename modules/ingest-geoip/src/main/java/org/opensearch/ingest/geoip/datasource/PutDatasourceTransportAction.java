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
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.ingest.geoip.datasource.common.DatasourceMetadata;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * GeoIP datasource creation transport action to be executed in cluster manager node
 */
public class PutDatasourceTransportAction extends TransportClusterManagerNodeAction<PutDatasourceRequest, AcknowledgedResponse> {
    private static final Logger LOGGER = LogManager.getLogger(PutDatasourceTransportAction.class);
    private final DatasourceMetadataService datasourceMetadataService;
    private final DatasourceTaskManager datasourceTaskManager;
    @Inject
    public PutDatasourceTransportAction(
        final ThreadPool threadPool,
        final ClusterService clusterService,
        final TransportService transportService,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final DatasourceMetadataService datasourceMetadataService,
        final DatasourceTaskManager datasourceTaskManager
    ) {
        super(
            PutDatasourceAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutDatasourceRequest::new,
            indexNameExpressionResolver
        );
        this.datasourceMetadataService = datasourceMetadataService;
        this.datasourceTaskManager = datasourceTaskManager;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(final StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(final PutDatasourceRequest request, final ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected void clusterManagerOperation(final PutDatasourceRequest request, final ClusterState state, final ActionListener<AcknowledgedResponse> listener)
        throws Exception {
        threadPool.generic().submit(() -> {
            try {
                datasourceMetadataService.createDatasourceIndexIfAbsent();
                datasourceMetadataService.addDatasourceIfAbsent(request, new ActionListener<>() {
                    @Override
                    public void onResponse(final DatasourceMetadata metadata) {
                        datasourceTaskManager.scheduleDatasourceUpdate(metadata);
                        listener.onResponse(new AcknowledgedResponse(true));
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        listener.onFailure(e);
                    }
                });
            } catch (Exception e) {
                LOGGER.error("Failed to create a datasource {}", request.getId(), e);
                listener.onFailure(new OpenSearchException("Failed to create a datasource"));
            }
        });
    }
}
