/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource;

import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.regex.Regex;
import org.opensearch.ingest.geoip.datasource.put.PutDatasourceRequest;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;

public class DatasourceService {
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    private final ClusterManagerTaskThrottler.ThrottlingKey putGeoIPDatasourceTaskKey;

    @Inject
    public DatasourceService(
        ClusterService clusterService,
        ThreadPool threadPool
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;

        putGeoIPDatasourceTaskKey = clusterService.registerClusterManagerTask("put-geoip-datasource", true);
    }

    public void add(PutDatasourceRequest request, ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask(
            "put-geoip-datasource-" + request.getId(),
            new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

                @Override
                protected AcknowledgedResponse newResponse(boolean acknowledged) {
                    return new AcknowledgedResponse(acknowledged);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    return innerAdd(request, currentState);
                }

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return putGeoIPDatasourceTaskKey;
                }
            }
        );
    }

    private ClusterState innerAdd(PutDatasourceRequest request, ClusterState currentState) {
        DatasourceMetadata currentDatasourceMetadata = currentState.metadata().custom(DatasourceMetadata.TYPE);
        if (currentDatasourceMetadata == null) {
            currentDatasourceMetadata = new DatasourceMetadata(new HashMap<>());
        }
        Map<String, Datasource> datasources = currentDatasourceMetadata.getDatasources();
        for (String datasourceKey : datasources.keySet()) {
            if (Regex.simpleMatch(request.getId(), datasourceKey)) {
                throw new ResourceAlreadyExistsException("datasource [{}] already exist", request.getId());
            }
        }
        final Map<String, Datasource> datasourcesCopy = new HashMap<>(datasources);
        Datasource datasource = new Datasource(request.getId());
        datasourcesCopy.put(request.getId(), datasource);
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metadata(
            Metadata.builder(currentState.getMetadata()).putCustom(DatasourceMetadata.TYPE, new DatasourceMetadata(datasourcesCopy)).build()
        );
        return newState.build();
    }
}
