/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource.put;

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
import org.opensearch.ingest.geoip.datasource.DatasourceService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

public class PutDatasourceTransportAction extends TransportClusterManagerNodeAction<PutDatasourceRequest, AcknowledgedResponse> {
    private final DatasourceService datasourceService;
    @Inject
    public PutDatasourceTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        DatasourceService datasourceService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
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
        this.datasourceService = datasourceService;
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
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void clusterManagerOperation(PutDatasourceRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
        throws Exception {
        datasourceService.add(request, listener);
    }
}
