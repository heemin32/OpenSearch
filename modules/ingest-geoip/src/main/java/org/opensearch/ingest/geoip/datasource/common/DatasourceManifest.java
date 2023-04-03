/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.CharBuffer;

/**
 * GeoIP datasource manifest file object
 *
 * Manifest file is stored in an external endpoint. OpenSearch read the file and store values it in this object.
 */
public class DatasourceManifest {
    @JsonProperty("url")
    private String url;
    @JsonProperty("db_name")
    private String dbName;
    @JsonProperty("md5_hash")
    private String md5Hash;
    @JsonProperty("valid_for")
    private long validFor;
    @JsonProperty("updated_at")
    private long updatedAt;
    @JsonProperty("provider")
    private String provider;

    public String getUrl() {
        return url;
    }

    public void setUrl(final String url) {
        this.url = url;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(final String dbName) {
        this.dbName = dbName;
    }

    public String getMd5Hash() {
        return md5Hash;
    }

    public void setMd5Hash(final String md5Hash) {
        this.md5Hash = md5Hash;
    }

    public long getValidFor() {
        return validFor;
    }

    public void setValidFor(final long validFor) {
        this.validFor = validFor;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(final long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public String getProvider() {
        return provider;
    }

    public void setProvider(final String provider) {
        this.provider = provider;
    }

    public static class Builder {
        private static final int MANIFEST_FILE_MAX_BYTES = 1024 * 5;
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        public static DatasourceManifest build(final URL url) throws IOException {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
                CharBuffer charBuffer = CharBuffer.allocate(MANIFEST_FILE_MAX_BYTES);
                reader.read(charBuffer);
                return OBJECT_MAPPER.readValue(charBuffer.flip().toString(), DatasourceManifest.class);
            }
        }
    }
}
