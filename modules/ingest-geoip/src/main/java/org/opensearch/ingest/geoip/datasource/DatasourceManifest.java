/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource;

import com.fasterxml.jackson.annotation.JsonProperty;

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
}
