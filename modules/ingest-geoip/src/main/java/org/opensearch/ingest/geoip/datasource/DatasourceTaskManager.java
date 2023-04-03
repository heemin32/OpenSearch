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
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.LocalNodeClusterManagerListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.ingest.geoip.datasource.common.DatasourceMetadata;
import org.opensearch.ingest.geoip.datasource.common.DatasourceState;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.ingest.geoip.IngestGeoIpModulePlugin.GEOIP_DATASOURCE_INDEX_NAME;

/**
 * A class to manage DatasourceTask
 */
public class DatasourceTaskManager extends AbstractLifecycleComponent implements LocalNodeClusterManagerListener, Runnable {
    private static final Logger LOGGER = LogManager.getLogger(DatasourceTaskManager.class);
    private static final String GEOIP_DS_UPDATER = "geoip_ds_updater";
    private static final int POOL_SIZE = 1;
    private static final int QUEUE_SIZE = 1000;
    private static final int SWEEP_PAGE_MAX_SIZE = 100;
    private static final TimeValue SWEEP_INTERVAL = TimeValue.timeValueMinutes(15l);
    private static final long TIME_DIFF_PRECISION_IN_SECOND = 10;
    private final ThreadPool threadPool;
    private final AtomicBoolean cancelled = new AtomicBoolean(true);
    private final ClusterService clusterService;
    private final Map<String, DatasourceTask> tasks;

    private final DatasourceMetadataService metadataService;
    private final DatasourceDatabaseService databaseService;

    private final Client client;

    private Scheduler.Cancellable scheduledSweep;

    @Inject
    public DatasourceTaskManager(final ThreadPool threadPool,
                                 final ClusterService clusterService,
                                 final Client client,
                                 final DatasourceMetadataService metadataService,
                                 final DatasourceDatabaseService databaseService) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.tasks = new ConcurrentHashMap<>();
        this.client = client;
        this.metadataService = metadataService;
        this.databaseService = databaseService;
    }

    public static List<ExecutorBuilder<?>> getExecutorBuilders(final Settings settings) {
        return Collections.singletonList(
            new FixedExecutorBuilder(settings, GEOIP_DS_UPDATER, POOL_SIZE, QUEUE_SIZE, "thread_pool." + GEOIP_DS_UPDATER)
        );
    }

    synchronized public void scheduleDatasourceUpdate(final DatasourceMetadata metadata) {
        LOGGER.debug("Scheduling datasource update for {} is requested", metadata.getId());
        if (cancelled.get()) {
            LOGGER.debug("Cancel is called. Skip scheduling datasource update for {}", metadata.getId());
            return;
        }
        DatasourceTask task = tasks.get(metadata.getId());
        if (task != null && task.isInProgress()) {
            LOGGER.debug("Update task is in progress. Skip scheduling datasource update for {}", metadata.getId());
            return;
        }

        if (DatasourceState.FAILED.equals(metadata.getState())) {
            LOGGER.debug("Datasource is already in FAILED state. Skip scheduling datasource update for {}", metadata.getId());
            if (task != null) {
                task.cancel();
            }
            return;
        }

        if (task == null) {
            LOGGER.debug("Scheduling datasource update for {}", metadata.getId());
            tasks.put(metadata.getId(), new DatasourceTask(metadata.getId(), client, clusterService, metadataService, databaseService));
            tasks.get(metadata.getId()).schedule(threadPool, metadata.getDelayFrom(Instant.now().getEpochSecond()), GEOIP_DS_UPDATER);
            return;
        }

        long actualDelay = task.getCancellable().getDelay(TimeUnit.SECONDS);
        long expectedDelay = metadata.getDelayFrom(Instant.now().getEpochSecond()).seconds();

        if (actualDelay < TIME_DIFF_PRECISION_IN_SECOND) {
            LOGGER.debug("The task will start in {} seconds. Skip scheduling datasource update for {}", actualDelay, metadata.getId());
            return;
        }

        if (Math.abs(actualDelay - expectedDelay) < TIME_DIFF_PRECISION_IN_SECOND) {
            LOGGER.debug("The task is scheduled with correct delay. Skip scheduling a new datasource update for {}", metadata.getId());
            return;
        }

        LOGGER.debug("Canceling the previous update task with delay {} seconds for {}", actualDelay, metadata.getId());
        task.cancel();
        LOGGER.debug("Scheduling a new datasource update with delay {} seconds for {}", expectedDelay, metadata.getId());
        tasks.put(metadata.getId(), new DatasourceTask(metadata.getId(), client, clusterService, metadataService, databaseService));
        tasks.get(metadata.getId()).schedule(threadPool, metadata.getDelayFrom(Instant.now().getEpochSecond()), GEOIP_DS_UPDATER);
    }

    @Override
    synchronized public void onClusterManager() {
        if (cancelled.compareAndSet(true, false)) {
            scheduledSweep = threadPool.scheduleWithFixedDelay(this, SWEEP_INTERVAL, ThreadPool.Names.GENERIC);
        }
    }

    @Override
    synchronized public void offClusterManager() {
        if (cancelled.compareAndSet(false, true)) {
            if (scheduledSweep != null) {
                scheduledSweep.cancel();
            }
            for (DatasourceTask task : tasks.values()) {
                task.cancel();
            }
        }
    }

    @Override
    protected void doStart() {
        if (DiscoveryNode.isClusterManagerNode(clusterService.getSettings())) {
            clusterService.addListener(this);
        }
    }

    @Override
    protected void doStop() {
        if (DiscoveryNode.isClusterManagerNode(clusterService.getSettings())) {
            clusterService.removeListener(this);
        }
    }

    @Override
    protected void doClose() throws IOException {
    }


    @Override
    public void run() {
        LOGGER.debug("GeoIP datasource sweeping for update started");
        tasks.entrySet().removeIf(e -> e.getValue().isCompleted());
        Map<String, Set<String>> datasourceNameToIndexVersions = null;
        String searchAfter = "";
        while (cancelled.get() == false) {
            List<DatasourceMetadata> metadataList = metadataService.sweepDatasourceMetadata(searchAfter, SWEEP_PAGE_MAX_SIZE);
            for (DatasourceMetadata metadata : metadataList) {
                scheduleDatasourceUpdate(metadata);
                datasourceNameToIndexVersions = createDatasourceNameToIndexVersions(datasourceNameToIndexVersions);
                deletePreviousDatabaseIndices(datasourceNameToIndexVersions, metadata);
            }
            if (metadataList.size() < SWEEP_PAGE_MAX_SIZE) {
                break;
            }
            searchAfter = metadataList.get(metadataList.size() - 1).getId();
        }
    }

    private Map<String, Set<String>> createDatasourceNameToIndexVersions(Map<String, Set<String>> data) {
        if (data != null) {
            return data;
        }
        data = new HashMap<>();
        Iterator<String> keyIt = clusterService.state().metadata().indices().keysIt();
        String datasourceIndexPrefix = GEOIP_DATASOURCE_INDEX_NAME + ".";
        while (keyIt.hasNext()) {
            String index = keyIt.next();
            if (index.startsWith(datasourceIndexPrefix)) {
                String indexBase = index.substring(0, index.lastIndexOf('.') + 1);
                String indexVersion = index.substring(index.lastIndexOf('.') + 1);
                data.putIfAbsent(indexBase, new HashSet<>());
                data.get(indexBase).add(indexVersion);
            }
        }
        return data;
    }

    private void deletePreviousDatabaseIndices(final Map<String, Set<String>> datasourceNameToIndexVersions, final DatasourceMetadata metadata) {
        if (metadata.getDatabase().getUpdatedAt() == null) {
            return;
        }
        List<String> indicesToDelete = new ArrayList<>();
        for (String version : datasourceNameToIndexVersions.get(metadata.getId())) {
            long versionNumber = Long.valueOf(version);
            if (versionNumber < metadata.getDatabase().getUpdatedAt()) {
                indicesToDelete.add(metadata.getIndexNameFor(versionNumber));
            }
        }
        if (indicesToDelete.isEmpty()) {
            LOGGER.debug("No previous index to delete for datasource {}", metadata.getId());
            return;
        }
        client.admin().indices().prepareDelete(indicesToDelete.toArray(new String[0])).execute(new ActionListener<>() {
            @Override
            public void onResponse(final AcknowledgedResponse acknowledgedResponse) {
                LOGGER.info("Successfully deleted previous indices {}", indicesToDelete);
            }

            @Override
            public void onFailure(final Exception e) {
                LOGGER.error("Failed to delete previous indices {}", indicesToDelete, e);
            }
        });
    }
}
