using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using Ozon.Gdz.Platform.Primitives;
using Ozon.Gdz.Warehouses.Contracts;
using Ozon.Gdz.Warehouses.Model;
using Ozon.Gdz.Platform.Serialization.Options;
using Ozon.Platform.Data.Abstractions;

namespace Ozon.Gdz.Warehouses.Repository;

public sealed class WarehouseRepository : IWarehouseRepository
{
    private const string _historyQuery = @"
    INSERT INTO warehouses_history (
                           warehouse_id,
                           warehouse_name,
                           warehouse_rezon_id,
                           warehouse_metazon_id,
                           warehouse_address,
                           warehouse_gln,
                           warehouse_type_id,
                           warehouse_created_by,
                           warehouse_sys_period,
                           warehouse_goodzon_id,
                           warehouse_characteristics)
    SELECT warehouses.warehouse_id,
           warehouses.warehouse_name,
           warehouses.warehouse_rezon_id,
           warehouses.warehouse_metazon_id,
           warehouses.warehouse_address,
           warehouses.warehouse_gln,
           warehouses.warehouse_type_id,
           warehouses.warehouse_created_by,
           tstzrange(lower(warehouses.warehouse_sys_period), CURRENT_TIMESTAMP),
           warehouses.warehouse_goodzon_id,
           warehouse_characteristics
    FROM warehouses";

    private readonly IPostgresConnectionFactory _connectionFactory;
    private readonly JsonSerializerOptions _serializerOptions;

    public WarehouseRepository(IPostgresConnectionFactory connectionFactory, IOptions<JsonOptions> serializerOptions)
    {
        _connectionFactory = connectionFactory;
        _serializerOptions = serializerOptions.Value.JsonSerializerOptions;
    }

    public async Task AddAsync(Warehouse warehouse, string createdBy, CancellationToken cancellationToken = default)
    {
        const string query = @"
INSERT INTO warehouses (warehouse_id,
                        warehouse_name,
                        warehouse_rezon_id,
                        warehouse_metazon_id,
                        warehouse_address,
                        warehouse_GLN,
                        warehouse_type_id,
                        warehouse_created_by,
                        warehouse_sys_period,
                        warehouse_characteristics,
                        warehouse_new_type,
                        warehouse_assignment,
                        warehouse_management_system)
VALUES (:warehouse_id,
        :warehouse_name,
        :warehouse_rezon_id,
        :warehouse_metazon_id,
        :warehouse_address,
        :warehouse_GLN,
        :warehouse_type_id,
        :warehouse_created_by,
        tstzrange(CURRENT_TIMESTAMP, NULL),
        :warehouse_characteristics,
        :warehouse_new_type,
        :warehouse_assignment,
        :warehouse_management_system)";

        await using var connection = await _connectionFactory.GetMasterReplicaAsync(cancellationToken);
        await using DbCommand command = new DbCommandInitializer(query, connection)
        {
            Parameters =
            {
                { "warehouse_id", warehouse.WarehouseId },
                { "warehouse_name", warehouse.Name },
                { "warehouse_rezon_id", warehouse.RezonId },
                { "warehouse_metazon_id", warehouse.MetazonId },
                { "warehouse_address", warehouse.Address },
                { "warehouse_GLN", warehouse.GLN },
                { "warehouse_type_id", warehouse.TypeId },
                { "warehouse_created_by", createdBy },
                {
                    "warehouse_characteristics",
                    JsonSerializer.Serialize(warehouse.Characteristics, _serializerOptions),
                    NpgsqlDbType.Jsonb
                },
                { "warehouse_new_type", warehouse.NewType },
                { "warehouse_assignment", warehouse.Assignment },
                { "warehouse_management_system", warehouse.ManagementSystem },
            }
        };

        await connection.OpenAsync(cancellationToken);
        await command.ExecuteScalarAsync(cancellationToken);
    }

    public IAsyncEnumerable<Warehouse> GetAsync(
        WarehouseTypeId[] typeIds,
        string? searchName = null,
        CancellationToken cancellationToken = default) =>
        GetAsync(Array.Empty<WarehouseId>(), typeIds, false, searchName, cancellationToken);

    public async Task<Warehouse> GetAsync(WarehouseId warehouseId, CancellationToken cancellationToken = default)
    {
        var warehouses = await GetAsync(new[] { warehouseId }, Array.Empty<WarehouseTypeId>(), true, null, cancellationToken)
            .ToArrayAsync(cancellationToken);

        return warehouses.SingleOrDefault()
               ?? throw new DataException($"Not found warehouse with id {warehouseId}.");
    }

    public IAsyncEnumerable<Warehouse> GetStorageWarehousesAsync(CancellationToken cancellationToken = default) =>
        GetAsync(WarehouseTypeRepository.StorageWarehouseTypeIds, null, cancellationToken);

    public async Task UpdateAsync(Warehouse warehouse, string updatedBy, CancellationToken cancellationToken = default)
    {
        const string query = @"
WITH updated AS (
    UPDATE  warehouses
    SET     warehouse_name = :warehouse_name,
            warehouse_type_id = :warehouse_type_id,
            warehouse_sys_period = tstzrange(CURRENT_TIMESTAMP, NULL),
            warehouse_created_by = :edited_by,
            warehouse_gln = :warehouse_gln,
            warehouse_address = :warehouse_address,
            warehouse_characteristics = :warehouse_characteristics
    WHERE   warehouse_id = :warehouse_id
    AND (warehouse_name != :warehouse_name
         OR
         warehouse_type_id != :warehouse_type_id
         OR
         warehouse_gln IS DISTINCT FROM :warehouse_gln
         OR
         warehouse_address != :warehouse_address
         OR
         warehouse_characteristics != :warehouse_characteristics)
    RETURNING warehouse_id
)
" + _historyQuery + @"
JOIN updated USING (warehouse_id)";

        await using var connection = await _connectionFactory.GetMasterReplicaAsync(cancellationToken);
        await using DbCommand command = new DbCommandInitializer(query, connection)
        {
            Parameters =
            {
                { "warehouse_id", warehouse.WarehouseId },
                { "warehouse_name", warehouse.Name },
                { "warehouse_type_id", warehouse.TypeId },
                { "edited_by", updatedBy },
                { "warehouse_gln", warehouse.GLN },
                { "warehouse_address", warehouse.Address },
                {
                    "warehouse_characteristics",
                    JsonSerializer.Serialize(warehouse.Characteristics, _serializerOptions),
                    NpgsqlDbType.Jsonb
                }
            }
        };

        await connection.OpenAsync(cancellationToken);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<WarehouseSynchronizationInfo?> TryGetForSynchronizationAsync(CancellationToken cancellationToken = default)
    {
        const string query = @"
SELECT warehouse_id,
       warehouse_name,
       warehouse_rezon_id,
       warehouse_metazon_id,
       warehouse_address,
       warehouse_GLN,
       warehouse_type_id,
       warehouse_characteristics,
       warehouse_goodzon_id,
       warehouse_synchronized_at
FROM warehouses
WHERE warehouse_is_synchronized = FALSE
ORDER BY warehouse_id ASC
LIMIT 1 FOR UPDATE SKIP LOCKED;";

        await using var connection = await _connectionFactory.GetMasterReplicaAsync(cancellationToken);
        await using DbCommand command = new DbCommandInitializer(query, connection);

        await connection.OpenAsync(cancellationToken);

        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow, cancellationToken);

        await reader.ReadAsync(cancellationToken);
        if (!reader.HasRows)
            return null;

        var warehouse = ReadWarehouse(reader);
        var warehouseSynchronizedAt = reader.GetFieldValue<DateTimeOffset?>(9);
        var syncState = warehouseSynchronizedAt is { }
            ? WarehouseSynchronizationInfo.SyncState.Outdated
            : WarehouseSynchronizationInfo.SyncState.Never;

        return new WarehouseSynchronizationInfo(warehouse, syncState);
    }

    public Task MarkAsSynchronizedAsync(WarehouseId id, long? goodzonId, CancellationToken cancellationToken = default) =>
        UpdateSynchronizationStateAsync(id, true, goodzonId, cancellationToken);

    public Task MarkAsShouldSynchronizeAsync(WarehouseId id, CancellationToken cancellationToken = default) =>
        UpdateSynchronizationStateAsync(id, false, null, cancellationToken);

    public async Task<long> CountNonSynchronizedAsync(CancellationToken cancellationToken = default)
    {
        const string query = "SELECT COUNT (*) FROM warehouses WHERE warehouse_is_synchronized = FALSE;";

        await using var connection = await _connectionFactory.GetMasterReplicaAsync(cancellationToken);
        await using DbCommand command = new DbCommandInitializer(query, connection);

        await connection.OpenAsync(cancellationToken);

        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow, cancellationToken);
        return await reader.ReadAsync(cancellationToken) && reader.FieldCount != 0
            ? reader.GetFieldValue<long>(0)
            : default;
    }

    private async IAsyncEnumerable<Warehouse> GetAsync(
        WarehouseId[] warehouseIds,
        WarehouseTypeId[] typeIds,
        bool singleRow,
        string? searchName = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        const string query = @"
SELECT warehouse_id,
       warehouse_name,
       warehouse_rezon_id,
       warehouse_metazon_id,
       warehouse_address,
       warehouse_GLN,
       warehouse_type_id,
       warehouse_characteristics,
       warehouse_goodzon_id
FROM warehouses
WHERE  (cardinality(:warehouse_ids) = 0 OR warehouse_id = ANY(:warehouse_ids))
       AND (cardinality(:typeIds) = 0 OR warehouse_type_id = ANY(:typeIds))
       AND (LOWER(TRIM(BOTH warehouse_name)) LIKE :searchName)
ORDER BY warehouse_name ASC;";

        await using var connection = await _connectionFactory.GetMasterReplicaAsync(cancellationToken);
        await using DbCommand command = new DbCommandInitializer(query, connection)
        {
            Parameters =
            {
                { "warehouse_ids", warehouseIds },
                { "typeIds", typeIds },
                { "searchName", $"%{searchName?.Trim().ToLower()}%" }
            }
        };

        await connection.OpenAsync(cancellationToken);
        await using var reader = await command.ExecuteReaderAsync(
            singleRow
                ? CommandBehavior.SingleRow
                : CommandBehavior.Default,
            cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
            yield return ReadWarehouse(reader);
    }

    private async Task UpdateSynchronizationStateAsync(
        WarehouseId id,
        bool isSynchronized,
        long? goodzonId = null,
        CancellationToken cancellationToken = default)
    {
        const string query = @"
UPDATE warehouses
SET warehouse_synchronized_at =
    CASE
        WHEN :warehouse_is_synchronized = TRUE THEN now() at time zone 'utc'
        ELSE warehouse_synchronized_at
    END,
    warehouse_is_synchronized = :warehouse_is_synchronized,
    warehouse_goodzon_id = COALESCE(:warehouse_goodzon_id, warehouse_goodzon_id)
WHERE warehouse_id = :warehouse_id;";

        await using var connection = await _connectionFactory.GetMasterReplicaAsync(cancellationToken);
        await using DbCommand command = new DbCommandInitializer(query, connection)
        {
            Parameters =
            {
                { "warehouse_id", id },
                { "warehouse_is_synchronized", isSynchronized },
                { "warehouse_goodzon_id", goodzonId }
            }
        };

        await connection.OpenAsync(cancellationToken);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async IAsyncEnumerable<WarehouseSaleRegions> GetSaleRegionsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        const string query = @"
WITH selection AS (
    SELECT p.warehouse_id,
           r.region_id
    FROM warehouse_priorities p
             JOIN regions r ON p.region_id = r.region_parent_id
                               OR p.region_id = r.region_id
)
SELECT warehouse_id,
       array_agg(region_id) AS warehouse_sales_regions_ids
FROM selection
GROUP BY warehouse_id";

        await using var connection = await _connectionFactory.GetMasterReplicaAsync(cancellationToken);
        await using DbCommand command = new DbCommandInitializer(query, connection);

        await connection.OpenAsync(cancellationToken);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            yield return new WarehouseSaleRegions(
                reader.GetFieldValue<WarehouseId>(0),
                reader.GetFieldValue<RegionId[]>(1));
    }

    public async IAsyncEnumerable<Warehouse> GetFilteredListAsync(
        WarehouseId warehouseId,
        string warehouseName,
        long rezonId,
        long metazonId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        const string query = @"
SELECT warehouse_id,
       warehouse_name,
       warehouse_rezon_id,
       warehouse_metazon_id,
       warehouse_address,
       warehouse_GLN,
       warehouse_type_id,
       warehouse_characteristics,
       warehouse_goodzon_id
FROM warehouses
WHERE warehouse_id = :warehouse_id
   OR
      warehouse_name = :warehouse_name
   OR
      warehouse_rezon_id = :warehouse_rezon_id
   OR
      warehouse_metazon_id = :warehouse_metazon_id;";

        await using var connection = await _connectionFactory.GetMasterReplicaAsync(cancellationToken);
        await using DbCommand command = new DbCommandInitializer(query, connection)
        {
            Parameters =
            {
                { "warehouse_id", warehouseId },
                { "warehouse_name", warehouseName },
                { "warehouse_rezon_id", rezonId },
                { "warehouse_metazon_id", metazonId }
            }
        };

        await connection.OpenAsync(cancellationToken);

        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow, cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            yield return ReadWarehouse(reader);
    }

    public async Task MarkNotSynchronizeWarehousesAsync(WarehouseId[] warehouseIds, CancellationToken cancellationToken = default)
    {
        const string query = "UPDATE warehouses SET warehouse_is_synchronized = FALSE WHERE warehouse_id = ANY(:warehouse_ids)";

        await using var connection = await _connectionFactory.GetMasterReplicaAsync(cancellationToken);
        await using DbCommand command = new DbCommandInitializer(query, connection)
        {
            Parameters =
            {
                { "warehouse_ids", warehouseIds }
            }
        };

        await connection.OpenAsync(cancellationToken);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<bool> IsNameDuplicatedAsync(
        WarehouseId warehouseId,
        string warehouseName,
        CancellationToken cancellationToken = default)
    {
        const string query = @"
SELECT warehouse_id FROM warehouses
WHERE warehouse_id != :warehouse_id AND warehouse_name = :warehouse_name LIMIT 1;";

        await using var connection = await _connectionFactory.GetMasterReplicaAsync(cancellationToken);
        await using DbCommand command = new DbCommandInitializer(query, connection)
        {
            Parameters =
            {
                { "warehouse_id", warehouseId },
                { "warehouse_name", warehouseName }
            }
        };

        await connection.OpenAsync(cancellationToken);
        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow, cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return reader.HasRows;
    }

    public IAsyncEnumerable<Warehouse> GetAsync(WarehouseId[] warehouseIds, CancellationToken cancellationToken) =>
        GetAsync(warehouseIds, Array.Empty<WarehouseTypeId>(), false, null, cancellationToken);

    public async IAsyncEnumerable<(WarehouseId WarehouseId, string Address)> GetWarehousesAddressesAsync(
        WarehouseId warehouseId,
        string gln,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        const string query = @"
SELECT warehouse_id, warehouse_address
FROM warehouses
WHERE warehouse_id != :warehouse_id
AND warehouse_GLN = :warehouse_GLN;";

        await using var connection = await _connectionFactory.GetMasterReplicaAsync(cancellationToken);
        await using DbCommand command = new DbCommandInitializer(query, connection)
        {
            Parameters =
            {
                { "warehouse_id", warehouseId },
                { "warehouse_GLN", gln }
            }
        };

        await connection.OpenAsync(cancellationToken);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            yield return (
                reader.GetFieldValue<WarehouseId>(0),
                reader.GetFieldValue<string>(1));
    }

    private Warehouse ReadWarehouse(DbDataReader reader) =>
        new(
            reader.GetFieldValue<WarehouseId>(0),
            reader.GetFieldValue<string>(1),
            reader.GetFieldValue<long>(2),
            reader.GetFieldValue<long>(3),
            reader.GetFieldValue<string>(4),
            reader.GetNullableString(5),
            reader.GetFieldValue<WarehouseTypeId>(6),
            reader.GetJson<WarehouseCharacteristics>(7, _serializerOptions)!,
            reader.GetFieldValue<long?>(8));
}
