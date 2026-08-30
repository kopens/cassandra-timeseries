/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.statements.schema;

import java.util.List;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.timeseries.tiering.ChunkTables;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.sequences.DropAccordTable.TableReference;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.PrepareDropAccordTable;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static java.lang.String.join;

public final class DropTableStatement extends AlterSchemaStatement
{
    private final String tableName;
    private final boolean ifExists;

    public DropTableStatement(String keyspaceName, String tableName, boolean ifExists)
    {
        super(keyspaceName);
        this.tableName = tableName;
        this.ifExists = ifExists;
    }

    @Override
    protected ClusterMetadata commit(ClusterMetadata metadata)
    {
        KeyspaceMetadata keyspace = metadata.schema.getKeyspaces().getNullable(keyspaceName);
        TableMetadata table = null == keyspace
                              ? null
                              : keyspace.getTableOrViewNullable(tableName);
        if (table == null // this can happen when ifExists=true... since its already been validated can skip
            || !table.requiresAccordSupport())
            return super.commit(metadata);

        // Multi-Step Operation
        // 1) mark the table as pending delete
        // 2) await for Accord to finish transactions
        // 3) drop table
        TableReference ref = TableReference.from(table);
        ClusterMetadataService.instance().commit(new PrepareDropAccordTable(ref));
        return InProgressSequences.finishInProgressSequences(ref);
    }

    public boolean compatibleWith(ClusterMetadata metadata)
    {
        return metadata.directory.commonSerializationVersion.isAtLeast(Version.V0);
    }

    public Keyspaces apply(ClusterMetadata metadata)
    {
        Guardrails.dropTruncateTableEnabled.ensureEnabled(state);

        Keyspaces schema = metadata.schema.getKeyspaces();
        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);

        TableMetadata table = null == keyspace
                            ? null
                            : keyspace.getTableOrViewNullable(tableName);

        if (null == table)
        {
            if (ifExists)
                return schema;

            throw ire("Table '%s.%s' doesn't exist", keyspaceName, tableName);
        }

        if (table.isView())
            throw ire("Cannot use DROP TABLE on a materialized view. Please use DROP MATERIALIZED VIEW instead.");

        if (table.requiresAccordSupport() && table.params.pendingDrop)
            throw ire("Table '%s.%s' is already being dropped", keyspaceName, tableName);

        Iterable<ViewMetadata> views = keyspace.views.forTable(table.id);
        if (!isEmpty(views))
        {
            throw ire("Cannot drop a table when materialized views still depend on it (%s)",
                      keyspaceName,
                      join(", ", transform(views, ViewMetadata::name)));
        }

        // A tiered table's chunk table holds the only copy of every row the re-encoder moved (the
        // base copies were deleted when they were encoded), and transparent reads resolve through
        // the *base* table's name -- so dropping the base while its shadows exist strands the cold
        // data with no error anywhere. Make destroying it an explicit act on the shadow tables.
        List<String> shadows = ChunkTables.existingShadowTables(keyspace, tableName);
        if (!shadows.isEmpty())
        {
            String chunkTable = ChunkTables.chunkTableName(tableName);
            String holdsData = shadows.contains(chunkTable)
                             ? format(" %s.%s holds the only copy of every tiered row -- the base copies were " +
                                      "deleted when they were encoded -- so this drop would strand that data " +
                                      "unreadably.", keyspaceName, chunkTable)
                             : "";
            throw ire("Cannot drop %s.%s: its time-series tiering shadow tables still exist (%s).%s " +
                      "First detach the tiering policy so the sweeper stops recreating them " +
                      "(ALTER TABLE %s.%s WITH extensions = {}), then DROP the shadow tables, " +
                      "then re-run this DROP.",
                      keyspaceName, tableName, join(", ", shadows), holdsData, keyspaceName, tableName);
        }

        // The reverse direction of the same hazard: dropping the chunk table out from under a base
        // table whose tiering policy is still attached destroys the only copy of the tiered rows,
        // and the sweeper then recreates the table empty -- so the loss is silent. Detaching the
        // policy first is the explicit destruction step the refusal demands.
        String chunkDropError = ChunkTables.chunkTableDropError(keyspace, tableName);
        if (chunkDropError != null)
            throw ire(chunkDropError);

        return schema.withAddedOrUpdated(keyspace.withSwapped(keyspace.tables.without(table)));
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.DROPPED, Target.TABLE, keyspaceName, tableName);
    }

    public void authorize(ClientState client)
    {
        client.ensureTablePermission(keyspaceName, tableName, Permission.DROP);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.DROP_TABLE, keyspaceName, tableName);
    }

    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, tableName);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final QualifiedName name;
        private final boolean ifExists;

        public Raw(QualifiedName name, boolean ifExists)
        {
            this.name = name;
            this.ifExists = ifExists;
        }

        public DropTableStatement prepare(ClientState state)
        {
            String keyspaceName = name.hasKeyspace() ? name.getKeyspace() : state.getKeyspace();
            return new DropTableStatement(keyspaceName, name.getName(), ifExists);
        }
    }
}
