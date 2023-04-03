/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource.common;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.ingest.geoip.IngestGeoIpModulePlugin;
import org.opensearch.ingest.geoip.datasource.PutDatasourceRequest;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.opensearch.ingest.geoip.datasource.common.DatasourceMetadata.Database.FIELDS_FIELD;
import static org.opensearch.ingest.geoip.datasource.common.DatasourceMetadata.Database.MD5_HASH_FIELD;
import static org.opensearch.ingest.geoip.datasource.common.DatasourceMetadata.Database.PROVIDER_FIELD;
import static org.opensearch.ingest.geoip.datasource.common.DatasourceMetadata.Database.UPDATED_AT_FIELD;

/**
 * GeoIP datasource metadata object
 */
public class DatasourceMetadata implements ToXContentObject {
    private static final ParseField ENDPOINT_FIELD = new ParseField("endpoint");
    private static final ParseField UPDATE_INTERVAL_FIELD = new ParseField("update_interval");
    private static final ParseField STATE_FIELD = new ParseField("state");
    private static final ParseField NEXT_UPDATE_FIELD = new ParseField("next_update");
    private static final ParseField EXPIRE_AT_FIELD = new ParseField("expire_at");
    private static final ParseField DATABASE_FIELD = new ParseField("database");

    private String id;
    private String endpoint;
    private TimeValue updateInterval;
    private DatasourceState state;
    private Long nextUpdate;
    private Long expireAt;

    private Database database;
    public static final ObjectParser<DatasourceMetadata, Void> PARSER;
    static {
        PARSER = new ObjectParser<>("datasource_metadata");
        PARSER.declareString((metadata, val) -> metadata.setEndpoint(val), ENDPOINT_FIELD);
        PARSER.declareLong((metadata, val) -> metadata.setUpdateInterval(TimeValue.timeValueDays(val)), UPDATE_INTERVAL_FIELD);
        PARSER.declareString((metadata, val) -> metadata.setState(DatasourceState.valueOf(val)), STATE_FIELD);
        PARSER.declareLong((metadata, val) -> metadata.setNextUpdate(val), NEXT_UPDATE_FIELD);
        PARSER.declareLong((metadata, val) -> metadata.setExpireAt(val), EXPIRE_AT_FIELD);
        PARSER.declareObject((metadata, val) -> metadata.setDatabase(val), Database.PARSER, DATABASE_FIELD);
    }

    public DatasourceMetadata(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(final String endpoint) {
        this.endpoint = endpoint;
    }

    public TimeValue getUpdateInterval() {
        return updateInterval;
    }

    public void setUpdateInterval(final TimeValue updateInterval) {
        this.updateInterval = updateInterval;
    }

    public DatasourceState getState() {
        return state;
    }

    public void setState(final DatasourceState state) {
        this.state = state;
    }

    public Long getNextUpdate() {
        return nextUpdate;
    }

    public void setNextUpdate(final Long nextUpdate) {
        this.nextUpdate = nextUpdate;
    }

    public Long getExpireAt() {
        return expireAt;
    }

    public void setExpireAt(final Long expireAt) {
        this.expireAt = expireAt;
    }

    public Database getDatabase() {
        return database;
    }

    public void setDatabase(final Database database) {
        this.database = database;
    }

    public String getIndexName() {
        return getIndexNameFor(getDatabase().updatedAt);
    }

    public String getIndexNameFor(long version) {
        return String.format("%s.%s.%d", IngestGeoIpModulePlugin.GEOIP_DATASOURCE_INDEX_NAME, getId(), version);
    }

    public TimeValue getDelayFrom(long epochTimeInSecond) {
        long nextUpdate = getNextUpdate() == null ? Long.MAX_VALUE : getNextUpdate();
        long expireAt = getExpireAt() == null ? Long.MAX_VALUE : getExpireAt();
        long nextEventTime = Math.min(nextUpdate, expireAt);
        return TimeValue.timeValueSeconds(Math.max(0l, nextEventTime - epochTimeInSecond));
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject()
            .field(ENDPOINT_FIELD.getPreferredName(), getEndpoint())
            .field(UPDATE_INTERVAL_FIELD.getPreferredName(), getUpdateInterval().days())
            .field(STATE_FIELD.getPreferredName(), getState().toString())
            .field(NEXT_UPDATE_FIELD.getPreferredName(), getNextUpdate())
            .field(EXPIRE_AT_FIELD.getPreferredName(), getExpireAt());

        builder.startObject(DATABASE_FIELD.getPreferredName())
            .field(PROVIDER_FIELD.getPreferredName(), database.getProvider())
            .field(MD5_HASH_FIELD.getPreferredName(), database.getMd5Hash())
            .field(UPDATED_AT_FIELD.getPreferredName(), database.getUpdatedAt())
            .startArray(FIELDS_FIELD.getPreferredName());

        for (String field : database.fields) {
            builder.value(field);
        }

        return builder.endArray()
            .endObject()
            .endObject();
    }

    public static class Builder {
        public static DatasourceMetadata build(final PutDatasourceRequest request) {
            DatasourceMetadata metadata = new DatasourceMetadata(request.getId());
            metadata.setEndpoint(request.getEndpoint());
            metadata.setUpdateInterval(request.getUpdateInterval());
            metadata.setState(DatasourceState.PREPARING);
            metadata.setNextUpdate(Instant.now().getEpochSecond());
            metadata.setExpireAt(Long.MAX_VALUE);
            metadata.setDatabase(new Database());
            return metadata;
        }
    }

    public static class Database {
        protected static final ParseField PROVIDER_FIELD = new ParseField("provider");
        protected static final ParseField MD5_HASH_FIELD = new ParseField("md5_hash");
        protected static final ParseField UPDATED_AT_FIELD = new ParseField("updated_at");
        protected static final ParseField FIELDS_FIELD = new ParseField("fields");

        private String provider;
        private String md5Hash;
        private Long updatedAt;
        private List<String> fields;

        public Database() {
            fields = Collections.emptyList();
        }
        public static final ObjectParser<Database, Void> PARSER;
        static {
            PARSER = new ObjectParser<>("datasource_metadata_database", true, Database::new);
            PARSER.declareStringOrNull((database, val) -> database.setProvider(val), PROVIDER_FIELD);
            PARSER.declareStringOrNull((database, val) -> database.setMd5Hash(val), MD5_HASH_FIELD);
            PARSER.declareLongOrNull((database, val) -> database.setUpdatedAt(val), 0l, UPDATED_AT_FIELD);
            PARSER.declareStringArray((database, val) -> database.setFields(val), FIELDS_FIELD);
        }

        public String getProvider() {
            return provider;
        }

        public void setProvider(final String provider) {
            this.provider = provider;
        }

        public String getMd5Hash() {
            return md5Hash;
        }

        public void setMd5Hash(final String md5Hash) {
            this.md5Hash = md5Hash;
        }

        public Long getUpdatedAt() {
            return updatedAt;
        }

        public void setUpdatedAt(final Long updatedAt) {
            this.updatedAt = updatedAt;
        }

        public List<String> getFields() {
            return fields;
        }

        public void setFields(final List<String> fields) {
            this.fields = fields;
        }
    }
}
