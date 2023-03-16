/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource;

import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.cluster.Diff;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ContextParser;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class Datasource extends AbstractDiffable<Datasource> implements ToXContentObject {

    private static final ObjectParser<Datasource.Builder, Void> PARSER = new ObjectParser<>("geoip_datasource_config", true, Datasource.Builder::new);
    static {
        PARSER.declareString(Datasource.Builder::setId, new ParseField("id"));
    }

    public static ContextParser<Void, Datasource> getParser() {
        return (parser, context) -> PARSER.apply(parser, null).build();
    }

    private static class Builder {

        private String id;
        void setId(String id) {
            this.id = id;
        }

        Datasource build() {
            return new Datasource(id);
        }
    }

    private final String id;
    private final String endpoint;
    private final TimeValue updateInterval;

    public Datasource(String id) {
        this.id = Objects.requireNonNull(id);
    }

    public String getId() {
        return id;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", id);
        builder.endObject();
        return builder;
    }

    public static Datasource readFrom(StreamInput in) throws IOException {
        return new Datasource(in.readString());
    }

    public static Diff<Datasource> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Datasource::readFrom, in);
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Datasource that = (Datasource) o;

        return id.equals(that.id);

    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
