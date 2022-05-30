using System;
using System.Linq;
using System.Threading.Tasks;
using AutoMapper;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.AspNetCore.Authorization;
using Ozon.Gdz.Platform.Grpc;
using Ozon.Gdz.Platform.Primitives;
using Ozon.Gdz.Warehouses.Contracts;
using Ozon.Gdz.Warehouses.Contracts.Regions;
using Ozon.Gdz.Warehouses.Model;
using Ozon.Gdz.Warehouses.Extensions;
using ListRegionsRequest = Ozon.Gdz.Warehouses.Contracts.Regions.ListRegionsRequest;
using ListRegionsResponse = Ozon.Gdz.Warehouses.Contracts.Regions.ListRegionsResponse;

namespace Ozon.Gdz.Warehouses.Controllers;

public sealed class RegionsGrpcController : RegionsService.RegionsServiceBase
{
    private readonly IWarehousePrioritiesRepository _warehousePrioritiesRepository;
    private readonly IRegionRepository _regionRepository;
    private readonly IMapper _mapper;

    public RegionsGrpcController(
        IRegionRepository regionRepository,
        IMapper mapper,
        IWarehousePrioritiesRepository warehousePrioritiesRepository)
    {
        _regionRepository = regionRepository;
        _mapper = mapper;
        _warehousePrioritiesRepository = warehousePrioritiesRepository;
    }

    [Authorize]
    public override async Task<CreateRegionResponse> CreateRegion(
        CreateRegionRequest request,
        ServerCallContext context)
    {
        var region = new Region(
            (RegionId)request.RegionId,
            request.Name,
            request.Title,
            (RegionId?)request.ParentId,
            (ClusterId)request.ClusterId,
            request.UpdatePrioritiesEnabled,
            context.GetPreferredUsername(),
            DateTimeOffset.UtcNow);

        if (string.IsNullOrEmpty(region.Name) || string.IsNullOrEmpty(region.Title))
            throw new ArgumentException($"Parameters {nameof(region.Name)} and {nameof(region.Title)} have to be filled");

        await _regionRepository.CreateAsync(region, context.CancellationToken);

        return new CreateRegionResponse();
    }

    public override async Task<ListRegionsResponse> ListRegions(
        ListRegionsRequest request,
        ServerCallContext context)
    {
        var result = await _regionRepository.ReadAsync(
                request.RegionIds.ToRegionIdsArray(),
                context.CancellationToken)
            .ToArrayAsync(context.CancellationToken);
        return _mapper.Map<ListRegionsResponse>(result);
    }

    [Authorize]
    [Transactional]
    public override async Task<UpdateRegionResponse> UpdateRegion(
        UpdateRegionRequest request,
        ServerCallContext context)
    {
        if (!FieldMask.IsValid<UpdateRegionRequest.Types.UpdateRegion>(request.UpdateMask))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid update_mask"));

        var existingRegion = await _regionRepository
                                 .ReadAsync(new[] { (RegionId)request.Region.RegionId }, context.CancellationToken)
                                 .SingleOrDefaultAsync(context.CancellationToken)
                             ?? throw new RpcException(
                                 new Status(StatusCode.NotFound, $"Region with id = {request.Region.RegionId} not found"));

        var editedBy = context.GetPreferredUsername();

        var updatedRegion = existingRegion with
        {
            Name = request.UpdateMask.Contains<UpdateRegionRequest.Types.UpdateRegion>(
                UpdateRegionRequest.Types.UpdateRegion.NameFieldNumber)
                ? request.Region.Name
                : existingRegion.Name,
            Title = request.UpdateMask.Contains<UpdateRegionRequest.Types.UpdateRegion>(
                UpdateRegionRequest.Types.UpdateRegion.TitleFieldNumber)
                ? request.Region.Title
                : existingRegion.Title,
            ClusterId = request.UpdateMask.Contains<UpdateRegionRequest.Types.UpdateRegion>(
                UpdateRegionRequest.Types.UpdateRegion.ClusterIdFieldNumber)
                ? (ClusterId)request.Region.ClusterId
                : existingRegion.ClusterId,
            ParentId = request.UpdateMask.Contains<UpdateRegionRequest.Types.UpdateRegion>(
                UpdateRegionRequest.Types.UpdateRegion.ParentIdFieldNumber)
                ? (RegionId?)request.Region.ParentId
                : existingRegion.ParentId,
            UpdatePrioritiesEnabled =
            request.UpdateMask.Contains<UpdateRegionRequest.Types.UpdateRegion>(
                UpdateRegionRequest.Types.UpdateRegion.UpdatePrioritiesEnabledFieldNumber)
                ? request.Region.UpdatePrioritiesEnabled
                : existingRegion.UpdatePrioritiesEnabled,
            EditedBy = editedBy,
            Updated = DateTimeOffset.UtcNow
        };

        var region = await _regionRepository.UpdateAsync(updatedRegion, context.CancellationToken);

        if (request.Region.UpdatePrioritiesEnabled && existingRegion.UpdatePrioritiesEnabled == false)
            await _warehousePrioritiesRepository.SyncRegionPrioritiesAsync(region.RegionId, editedBy, context.CancellationToken);

        return new UpdateRegionResponse();
    }

    [Authorize]
    public override async Task<DeleteRegionsResponse> DeleteRegions(
        DeleteRegionsRequest request,
        ServerCallContext context)
    {
        var regionIds = request.RegionIds.ToRegionIdsArray();
        await _regionRepository.DeleteAsync(regionIds, context.CancellationToken);
        return new DeleteRegionsResponse();
    }
}
