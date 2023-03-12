using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Runtime.CompilerServices;


public sealed class RegionRepository : IRegionRepository
{
    private readonly IPostgresConnectionFactory _connectionFactory;

    public RegionRepository(IPostgresConnectionFactory connectionFactory) =>
        _connectionFactory = connectionFactory;

    public async ValueTask CreateAsync(Region region, CancellationToken cancellationToken)
    {
        const string query = @"
INSERT INTO regions
VALUES(:region_id,
       :region_name,
       :region_title,
       :region_parent_id,
       :cluster_id,
       :region_update_priorities_enabled,
       :region_edited_by,
       :region_updated);";

        await using var connection = await _connectionFactory.GetMasterReplicaAsync(cancellationToken);
        await using DbCommand command = new DbCommandInitializer(query, connection)
        {
            Parameters =
            {
                { "region_id", region.RegionId },
                { "region_name", region.Name },
                { "region_title", region.Title },
                { "region_parent_id", region.ParentId },
                { "cluster_id", region.ClusterId },
                { "region_update_priorities_enabled", region.UpdatePrioritiesEnabled },
                { "region_edited_by", region.EditedBy },
                { "region_updated", region.Updated }
            }
        };

        await connection.OpenAsync(cancellationToken);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async IAsyncEnumerable<Region> ReadAsync(
        RegionId[] regionIds,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        const string query = @"
SELECT region_id,
       region_name,
       region_title,
       region_parent_id,
       cluster_id,
       region_update_priorities_enabled,
       region_edited_by,
       region_updated
FROM regions
WHERE cardinality(:region_ids) = 0 OR region_id = ANY(:region_ids)
ORDER BY region_id;";

        await using var connection = await _connectionFactory.GetMasterReplicaAsync(cancellationToken);
        await using DbCommand command = new DbCommandInitializer(query, connection)
        {
            Parameters =
            {
                { "region_ids", regionIds }
            }
        };

        await connection.OpenAsync(cancellationToken);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
            yield return ReadRegion(reader);
    }

    public async IAsyncEnumerable<Region> ReadAsync(
        ClusterId[] clusterIds,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        const string query = @"
SELECT region_id,
       region_name,
       region_title,
       region_parent_id,
       cluster_id,
       region_update_priorities_enabled,
       region_edited_by,
       region_updated
FROM regions
WHERE cardinality(:cluster_ids) = 0 OR cluster_id = ANY(:cluster_ids)
ORDER BY region_id;";

        await using var connection = await _connectionFactory.GetMasterReplicaAsync(cancellationToken);
        await using DbCommand command = new DbCommandInitializer(query, connection)
        {
            Parameters =
            {
                { "cluster_ids", clusterIds }
            }
        };

        await connection.OpenAsync(cancellationToken);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
            yield return ReadRegion(reader);
    }

    public async ValueTask<Region> UpdateAsync(Region region, CancellationToken cancellationToken)
    {
        const string query = @"
WITH history AS (
    INSERT INTO regions_history(region_id,
                                region_name,
                                region_title,
                                region_parent_id,
                                cluster_id,
                                region_update_priorities_enabled,
                                region_edited_by,
                                region_updated)
    SELECT region_id,
           region_name,
           region_title,
           region_parent_id,
           cluster_id,
           region_update_priorities_enabled,
           region_edited_by,
           region_updated
    FROM regions
    WHERE region_id = :region_id
)
UPDATE regions
    SET region_name = COALESCE(:region_name, region_name),
        region_title = COALESCE(:region_title, region_title),
        region_parent_id = :region_parent_id,
        cluster_id = :cluster_id,
        region_update_priorities_enabled = :region_update_priorities_enabled,
        region_edited_by = :region_edited_by,
        region_updated = :region_updated
    WHERE region_id = :region_id
    RETURNING *";

        await using var connection = await _connectionFactory.GetMasterReplicaAsync(cancellationToken);
        await using DbCommand command = new DbCommandInitializer(query, connection)
        {
            Parameters =
            {
                { "region_id", region.RegionId },
                { "region_name", string.IsNullOrEmpty(region.Name) ? null : region.Name },
                { "region_title", string.IsNullOrEmpty(region.Title) ? null : region.Title },
                { "region_parent_id", region.ParentId },
                { "cluster_id", region.ClusterId },
                { "region_update_priorities_enabled", region.UpdatePrioritiesEnabled },
                { "region_edited_by", region.EditedBy },
                { "region_updated", region.Updated },
            }
        };

        await connection.OpenAsync(cancellationToken);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return ReadRegion(reader);
    }

    public async ValueTask DeleteAsync(RegionId[] regionIds, CancellationToken cancellationToken)
    {
        const string query = @"
WITH history AS (
    INSERT INTO regions_history(region_id,
                                region_name,
                                region_title,
                                region_parent_id,
                                cluster_id,
                                region_update_priorities_enabled,
                                region_edited_by,
                                region_updated)
    SELECT region_id,
           region_name,
           region_title,
           region_parent_id,
           cluster_id,
           region_update_priorities_enabled,
           region_edited_by,
           region_updated
    FROM regions WHERE region_id = ANY(:region_ids)
)
DELETE FROM regions
WHERE region_id = ANY(:region_ids);";

        await using var connection = await _connectionFactory.GetMasterReplicaAsync(cancellationToken);
        await using DbCommand command = new DbCommandInitializer(query, connection)
        {
            Parameters =
            {
                { "region_ids", regionIds }
            }
        };

        await connection.OpenAsync(cancellationToken);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async IAsyncEnumerable<Region> GetAllAsync(
        WarehouseId[] warehouseIds,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        const string query = @"
SELECT r.region_id,
       r.region_name,
       r.region_title,
       r.region_parent_id,
       r.cluster_id,
       r.region_update_priorities_enabled,
       r.region_edited_by,
       r.region_updated
FROM regions r
WHERE (cardinality(:warehouse_ids) = 0 OR
       region_id IN  (
           SELECT region_id
           FROM region_home_warehouses
           WHERE warehouse_id = ANY(:warehouse_ids))
       )
ORDER BY region_title;";

        await using var connection = await _connectionFactory.GetMasterReplicaAsync(cancellationToken);
        await using DbCommand command = new DbCommandInitializer(query, connection)
        {
            Parameters =
            {
                { "warehouse_ids", warehouseIds }
            }
        };

        await connection.OpenAsync(cancellationToken);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            yield return ReadRegion(reader);
    }

    private static Region ReadRegion(DbDataReader reader) =>
        new(
            reader.GetFieldValue<RegionId>(0),
            reader.GetFieldValue<string>(1),
            reader.GetFieldValue<string>(2),
            reader.GetFieldValue<RegionId?>(3),
            reader.GetFieldValue<ClusterId>(4),
            reader.GetFieldValue<bool>(5),
            reader.GetFieldValue<string>(6),
            reader.GetFieldValue<DateTimeOffset>(7));
}
