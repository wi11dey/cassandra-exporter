package com.zegelin.cassandra.exporter;

import com.zegelin.cassandra.exporter.MetadataFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.utils.FBUtilities;

import java.net.InetAddress;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class InternalMetadataFactory extends MetadataFactory {
    private static Optional<CFMetaData> getCFMetaData(final String keyspaceName, final String tableName) {
        return Optional.ofNullable(Schema.instance.getCFMetaData(keyspaceName, tableName));
    }

    @Override
    public Optional<IndexMetadata> indexMetadata(final String keyspaceName, final String tableName, final String indexName) {
        return getCFMetaData(keyspaceName, tableName)
                .map(Stream::of)
                .orElseGet(Stream::empty)
                .flatMap(m -> m.allColumns().stream())
                .filter(m -> indexName.equals(m.getIndexName()))
                .<IndexMetadata>map(m -> {
                    final IndexMetadata.IndexType indexType = IndexMetadata.IndexType.valueOf(m.getIndexType().toString());
                    final Optional<String> className = Optional.ofNullable(m.getIndexOptions().get("class_name"));

                    return new IndexMetadata() {
                        @Override
                        public IndexType indexType() {
                            return indexType;
                        }

                        @Override
                        public Optional<String> customClassName() {
                            return className;
                        }
                    };
                })
                .findAny();
    }

    @Override
    public Optional<TableMetadata> tableOrViewMetadata(final String keyspaceName, final String tableOrViewName) {
        return getCFMetaData(keyspaceName, tableOrViewName)
                .map(m -> new TableMetadata() {
                    @Override
                    public String compactionStrategyClassName() {
                        return m.compactionStrategyClass.getCanonicalName();
                    }

                    @Override
                    public boolean isView() {
                        return false;
                    }
                });
    }

    @Override
    public Set<String> keyspaces() {
        return Schema.instance.getKeyspaces();
    }

    @Override
    public Optional<EndpointMetadata> endpointMetadata(final InetAddress endpoint) {
        final IEndpointSnitch endpointSnitch = DatabaseDescriptor.getEndpointSnitch();

        return Optional.of(new EndpointMetadata() {
            @Override
            public String dataCenter() {
                return endpointSnitch.getDatacenter(endpoint);
            }

            @Override
            public String rack() {
                return endpointSnitch.getRack(endpoint);
            }
        });
    }

    @Override
    public String clusterName() {
        return DatabaseDescriptor.getClusterName();
    }

    @Override
    public InetAddress localBroadcastAddress() {
        return FBUtilities.getBroadcastAddress();
    }
}
