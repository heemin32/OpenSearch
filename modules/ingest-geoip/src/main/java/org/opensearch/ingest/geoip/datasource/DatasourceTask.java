/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.client.Requests;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.ingest.geoip.datasource.common.DatasourceManifest;
import org.opensearch.ingest.geoip.datasource.common.DatasourceMetadata;
import org.opensearch.ingest.geoip.datasource.common.DatasourceState;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Datasource database update task
 *
 * This is a background task which is responsible for creating/updating/deleting GeoIP database
 */
public class DatasourceTask implements Runnable {
    private static final Logger LOGGER = LogManager.getLogger(DatasourceTask.class);
    private static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(30);
    private final String id;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private volatile boolean started = false;
    private volatile boolean completed = false;

    private final Client client;
    private final ClusterService clusterService;
    private final DatasourceMetadataService metadataService;
    private final DatasourceDatabaseService databaseService;
    private Scheduler.ScheduledCancellable cancellable;

    public DatasourceTask(final String id,
                          final Client client,
                          final ClusterService clusterService,
                          final DatasourceMetadataService metadataService,
                          final DatasourceDatabaseService databaseService) {
        this.id = id;
        this.client = client;
        this.clusterService = clusterService;
        this.metadataService = metadataService;
        this.databaseService = databaseService;
    }

    @Override
    public void run() {
        started = true;
        if (cancelled.get()) {
            return;
        }

        DatasourceMetadata metadata = null;
        try {
            metadata = metadataService.getMetadata(id, DEFAULT_TIMEOUT);
            if (DatasourceState.PREPARING.equals(metadata.getState())
            || DatasourceState.AVAILABLE.equals(metadata.getState())) {
                updateDatasource(metadata);
            }
            else if (DatasourceState.DELETING.equals(metadata.getState())) {
                //TODO: implement
            }
            else {
                LOGGER.error("Skip. Invalid state is provided {}", metadata.getState());
            }
        } catch (Exception e) {
            LOGGER.error("Failed to update database for a datasource {}", id, e);
            if (metadata == null) {
                return;
            }

            if (DatasourceState.PREPARING.equals(metadata.getState())) {
                metadata.setState(DatasourceState.FAILED);
            }
            else if (DatasourceState.AVAILABLE.equals(metadata.getState())){
                Instant nextUpdate = Instant.now().plusSeconds(metadata.getUpdateInterval().seconds());
                metadata.setNextUpdate(nextUpdate.getEpochSecond());
            }
            else {
                // Do nothing
            }

            try {
                metadataService.updateMetadata(metadata);
            } catch (IOException ioe) {
                LOGGER.error("Failed to update datasource metadata {}", id, e);
            }
        } finally {
            completed = true;
        }
    }

    public void cancel() {
        if (cancelled.compareAndSet(false, true)) {
            if (cancellable != null) {
                cancellable.cancel();
            }
        }
    }

    public boolean isCompleted() {
        return cancelled.get() || completed;
    }

    public boolean isInProgress() {
        return started == true && isCompleted() == false;
    }

    public Scheduler.ScheduledCancellable getCancellable() {
        return cancellable;
    }

    public void schedule(final ThreadPool threadPool, final TimeValue delay, final String executor) {
        cancellable = threadPool.schedule(this, delay, executor);
    }

    private void updateDatasource(final DatasourceMetadata metadata) throws IOException {
        Instant start = Instant.now();
        if (cancelled.get()) {
            return;
        }

        URL url = new URL(metadata.getEndpoint());
        DatasourceManifest manifest = DatasourceManifest.Builder.build(url);

        if (shouldUpdate(metadata, manifest) == false) {
            Instant nextUpdate = Instant.now().plusSeconds(metadata.getUpdateInterval().seconds());
            metadata.setNextUpdate(nextUpdate.getEpochSecond());
            metadataService.updateMetadata(metadata);
            LOGGER.info("Skipping GeoIP database update. Update is not required for {}", metadata.getId());
            return;
        }

        databaseService.createNextIndex(metadata.getIndexNameFor(manifest.getUpdatedAt()), DEFAULT_TIMEOUT);
        String[] fields = null;
        boolean failed = false;
        try(CSVParser reader = databaseService.getDatabaseReader(manifest)) {
            BulkRequest bulkRequest = new BulkRequest();
            int bulkSize = 10000;
            int count = 0;
            for (CSVRecord record : reader) {
                if (cancelled.get()) {
                    return;
                }
                if (record.getRecordNumber() == 1) {
                    fields = record.values();
                    if (metadata.getDatabase().getFields().isEmpty() == false
                    && metadata.getDatabase().getFields().equals(Arrays.asList(fields)) == false) {
                        LOGGER.error("The previous fields and new fields does not match.");
                        LOGGER.error("Previous: {}, New: {}",
                            metadata.getDatabase().getFields().toString(),
                            Arrays.asList(fields).toString());
                        failed = true;
                        break;
                    }
                }
                else {
                    String document = databaseService.createDocument(fields, record.values());
                    IndexRequest request = Requests.indexRequest(metadata.getIndexNameFor(manifest.getUpdatedAt())).source(document, XContentType.JSON);
                    bulkRequest.add(request);
                    count++;
                    try {
                        if (count == bulkSize) {
                            bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
                            BulkResponse response = client.bulk(bulkRequest).get(30, TimeUnit.SECONDS);
                            if (response.hasFailures()) {
                                LOGGER.error("Error while ingesting GeoIP data for {} with an error {}", metadata.getId(), response.buildFailureMessage());
                                failed = true;
                                break;
                            }
                            bulkRequest = new BulkRequest();
                            count = 0;
                        }
                    } catch (InterruptedException|ExecutionException|TimeoutException e) {
                        LOGGER.error("Error while ingesting GeoIP data for {}", metadata.getId(), e);
                        failed = true;
                        break;
                    }
                }
            }
        }

        if (failed) {
            if (DatasourceState.PREPARING.equals(metadata.getState())) {
                metadata.setState(DatasourceState.FAILED);
            }
        }
        else {
            metadata.getDatabase().setProvider(manifest.getProvider());
            metadata.getDatabase().setMd5Hash(manifest.getMd5Hash());
            metadata.getDatabase().setUpdatedAt(manifest.getUpdatedAt());
            metadata.getDatabase().setFields(Arrays.asList(fields));
            metadata.setExpireAt(Instant.now().plusSeconds(TimeValue.timeValueDays(manifest.getValidFor()).seconds()).getEpochSecond());
            metadata.setState(DatasourceState.AVAILABLE);
        }
        Instant nextUpdate = Instant.now().plusSeconds(metadata.getUpdateInterval().seconds());
        metadata.setNextUpdate(nextUpdate.getEpochSecond());
        metadataService.updateMetadata(metadata);
        LOGGER.info("GeoIP database update {} for {} after {}",
            failed ? "failed" : "succeeded", metadata.getId(), Duration.between(start, Instant.now()));
    }

    private boolean shouldUpdate(final DatasourceMetadata metadata, final DatasourceManifest manifest) {
        if (DatasourceState.PREPARING.equals(metadata.getState())) {
            return true;
        }

        if (manifest.getMd5Hash().equals(metadata.getDatabase().getMd5Hash())) {
            return false;
        }

        return metadata.getDatabase().getUpdatedAt() <= manifest.getUpdatedAt();
    }
}
