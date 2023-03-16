/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource;

import org.opensearch.Version;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds the geoip datasource that are available in the cluster
 */
public class DatasourceMetadata implements Metadata.Custom {

    public static final String TYPE = "geoip-datasource";
    private static final ParseField DATA_SOURCE = new ParseField("datasource");
    private static final ObjectParser<List<Datasource>, Void> DATASOURCE_METADATA_PARSER = new ObjectParser<>(
        "datasource_metadata",
        ArrayList::new
    );

    static {
        DATASOURCE_METADATA_PARSER.declareObjectArray(List::addAll, Datasource.getParser(), DATA_SOURCE);
    }

    private final Map<String, Datasource> datasources;

    public DatasourceMetadata(Map<String, Datasource> datasources) {
        this.datasources = Collections.unmodifiableMap(datasources);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    public Map<String, Datasource> getDatasources() {
        return datasources;
    }

    public DatasourceMetadata(StreamInput in) throws IOException {
        int size = in.readVInt();
        Map<String, Datasource> datasources = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            Datasource datasource = Datasource.readFrom(in);
            datasources.put(datasource.getId(), datasource);
        }
        this.datasources = Collections.unmodifiableMap(datasources);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(datasources.size());
        for (Datasource datasource : datasources.values()) {
            datasource.writeTo(out);
        }
    }

    public static DatasourceMetadata fromXContent(XContentParser parser) throws IOException {
        Map<String, Datasource> datasources = new HashMap<>();
        List<Datasource> datasourceList = DATASOURCE_METADATA_PARSER.parse(parser, null);
        for (Datasource datasource : datasourceList) {
            datasources.put(datasource.getId(), datasource);
        }
        return new DatasourceMetadata(datasources);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(DATA_SOURCE.getPreferredName());
        for (Datasource pipeline : datasources.values()) {
            pipeline.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom before) {
        return new DatasourceMetadata.DatasourceMetadataDiff((DatasourceMetadata) before, this);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new DatasourceMetadata.DatasourceMetadataDiff(in);
    }

    static class DatasourceMetadataDiff implements NamedDiff<Metadata.Custom> {

        final Diff<Map<String, Datasource>> datasources;

        DatasourceMetadataDiff(DatasourceMetadata before, DatasourceMetadata after) {
            this.datasources = DiffableUtils.diff(before.datasources, after.datasources, DiffableUtils.getStringKeySerializer());
        }

        DatasourceMetadataDiff(StreamInput in) throws IOException {
            datasources = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                Datasource::readFrom,
                Datasource::readDiffFrom
            );
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new DatasourceMetadata(datasources.apply(((DatasourceMetadata) part).datasources));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            datasources.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DatasourceMetadata that = (DatasourceMetadata) o;

        return datasources.equals(that.datasources);

    }

    @Override
    public int hashCode() {
        return datasources.hashCode();
    }
}
