using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using AutoMapper;
using Grpc.Core;
using Microsoft.AspNetCore.Authorization;
using Ozon.Gdz.Platform.Grpc;
using Ozon.Gdz.Platform.Primitives;
using Ozon.Gdz.Warehouses.Contracts;
using Ozon.Gdz.Warehouses.Extensions;
using Ozon.Gdz.Warehouses.Helpers;
using Ozon.Gdz.Warehouses.Model;
using WarehouseAttributes = Ozon.Gdz.Warehouses.Model.WarehouseAttributes;
using WarehouseDetails = Ozon.Gdz.Warehouses.Model.WarehouseDetails;
using ProtoWarehouseType = Ozon.Gdz.Warehouses.Contracts.WarehouseType;
using WarehouseCharacteristics = Ozon.Gdz.Warehouses.Model.WarehouseCharacteristics;
using ProtoWarehouseCharacteristics = Ozon.Gdz.Warehouses.Contracts.WarehouseCharacteristics;

namespace Ozon.Gdz.Warehouses.Controllers;

public sealed class WarehousesGrpcController : WarehousesService.WarehousesServiceBase
{
    private readonly IWarehouseRepository _warehouseRepository;
    private readonly IWarehouseTypeRepository _warehouseTypeRepository;
    private readonly IRegionRepository _regionRepository;
    private readonly IWarehousePrioritiesRepository _warehousePrioritiesRepository;
    private readonly IRegionHomeWarehousesRepository _regionHomeWarehousesRepository;
    private readonly IMapper _mapper;
    private readonly IWhcWarehousesService _whcWarehousesService;

    public WarehousesGrpcController(
        IWarehouseRepository warehouseRepository,
        IWarehouseTypeRepository warehouseTypeRepository,
        IRegionRepository regionRepository,
        IWarehousePrioritiesRepository warehousePrioritiesRepository,
        IMapper mapper,
        IRegionHomeWarehousesRepository regionHomeWarehousesRepository,
        IWhcWarehousesService whcWarehousesService)
    {
        _warehouseRepository = warehouseRepository;
        _warehouseTypeRepository = warehouseTypeRepository;
        _regionRepository = regionRepository;
        _warehousePrioritiesRepository = warehousePrioritiesRepository;
        _mapper = mapper;
        _regionHomeWarehousesRepository = regionHomeWarehousesRepository;
        _whcWarehousesService = whcWarehousesService;
    }

    [Authorize]
    public override async Task<CreateWarehouseResponse> CreateWarehouse(CreateWarehouseRequest request, ServerCallContext context)
    {
        var warehouse = WarehouseBuilder.CreateWarehouse(request);

        var (isValid, message) = WarehouseValidationHelper.Validate(warehouse, (RegionId?)request.RegionId);
        if (!isValid)
            throw new RpcException(new Status(StatusCode.InvalidArgument, message!), message!);

        var (isNotDuplicated, duplicateMessage) = await CheckDuplicateRecordsAsync(warehouse, context.CancellationToken);
        if (!isNotDuplicated)
            throw new RpcException(new Status(StatusCode.InvalidArgument, duplicateMessage!), duplicateMessage!);

        if (!string.IsNullOrWhiteSpace(request.Gln))
        {
            var (isGlnValid, glnValidMessage) =
                await ValidateGlnAsync(
                    request.WarehouseId,
                    request.Address,
                    request.Gln.Trim(),
                    context.CancellationToken);
            if (!isGlnValid)
                throw new RpcException(new Status(StatusCode.InvalidArgument, glnValidMessage!), glnValidMessage!);
        }

        using var transactionScope = new TransactionScope(
            TransactionScopeOption.Required,
            new TransactionOptions { IsolationLevel = IsolationLevel.ReadCommitted },
            TransactionScopeAsyncFlowOption.Enabled);

        await _warehouseRepository.AddAsync(warehouse, context.GetPreferredUsername(), context.CancellationToken);

        if (request.RegionId != null)
            await _regionHomeWarehousesRepository.UpsertAsync(
                new[] { new RegionWarehouse((RegionId)request.RegionId.Value, (WarehouseId)request.WarehouseId) },
                context.GetPreferredUsername(),
                context.CancellationToken);

        transactionScope.Complete();

        return new CreateWarehouseResponse();
    }

    public override async Task<ListWarehousesResponse> ListWarehouses(ListWarehousesRequest request, ServerCallContext context)
    {
        var warehouses = await _warehouseRepository.GetAsync(
                request.TypeIds.Select(id => (WarehouseTypeId)id).ToArray(),
                request.SearchName,
                context.CancellationToken)
            .ToArrayAsync(context.CancellationToken);

        return _mapper.Map<IEnumerable<Warehouse>, ListWarehousesResponse>(warehouses);
    }

    public override async Task<ListWarehousesResponse> ListWarehousesV2(ListWarehousesRequestV2 request, ServerCallContext context)
    {
        var warehouses = await _warehouseRepository.GetAsync(
                request.WarehouseTypes
                    .Select(protoType => new WarehouseTypeId(_mapper.Map<WarehouseTypes>(protoType)))
                    .ToArray(),
                request.SearchName,
                context.CancellationToken)
            .ToArrayAsync(context.CancellationToken);

        return _mapper.Map<IEnumerable<Warehouse>, ListWarehousesResponse>(warehouses);
    }

    public override async Task<ListStorageWarehousesResponse> ListStorageWarehouses(
        ListStorageWarehousesRequest request,
        ServerCallContext context)
    {
        var warehouses = await _warehouseRepository
            .GetStorageWarehousesAsync(context.CancellationToken)
            .ToArrayAsync(context.CancellationToken);

        return _mapper.Map<IEnumerable<Warehouse>, ListStorageWarehousesResponse>(warehouses);
    }

    public override async Task<ListWarehouseTypesResponse> ListWarehouseTypes(ListWarehouseTypesRequest request, ServerCallContext context)
    {
        var warehouseTypes = await _warehouseTypeRepository.GetAllAsync(context.CancellationToken).ToArrayAsync(context.CancellationToken);

        return _mapper.Map<IEnumerable<WarehouseType>, ListWarehouseTypesResponse>(warehouseTypes);
    }

    public override async Task<ListRegionsResponse> ListRegions(ListRegionsRequest request, ServerCallContext context)
    {
        var regions = await _regionRepository
            .GetAllAsync(request.WarehouseIds.ToWarehouseIdsArray(), context.CancellationToken)
            .ToArrayAsync(context.CancellationToken);

        return _mapper.Map<IEnumerable<Region>, ListRegionsResponse>(regions);
    }

    public override async Task<UpdateWarehouseResponse> UpdateWarehouses(UpdateWarehouseRequest request, ServerCallContext context)
    {
        var (isNameDuplicated, nameDuplicateMessage) =
            await CheckNameDuplicatedAsync((WarehouseId)request.WarehouseId, request.Name, context.CancellationToken);
        if (!isNameDuplicated)
            throw new RpcException(new Status(StatusCode.InvalidArgument, nameDuplicateMessage!), nameDuplicateMessage!);

        const string addressMessage = "Значение поля \"Юридический адрес\" введено неверно.";
        if (string.IsNullOrWhiteSpace(request.Address))
            throw new RpcException(new Status(StatusCode.InvalidArgument, addressMessage), addressMessage);

        if (!string.IsNullOrWhiteSpace(request.Gln))
        {
            var (isGlnValid, glnValidMessage) =
                await ValidateGlnAsync(
                    request.WarehouseId,
                    request.Address,
                    request.Gln.Trim(),
                    context.CancellationToken);
            if (!isGlnValid)
                throw new RpcException(new Status(StatusCode.InvalidArgument, glnValidMessage!), glnValidMessage!);
        }

        var (isValid, message) =
            WarehouseValidationHelper.CheckAbilityRegionAssignment((RegionId?)request.RegionId, request.TypeId);
        if (!isValid)
            throw new RpcException(new Status(StatusCode.InvalidArgument, message!), message!);

        var warehouseCharacteristics = new WarehouseCharacteristics(
            request.Characteristics.BarcodeOnly,
            request.Characteristics.WmsSystem,
            request.Characteristics.TransitWarehouse,
            request.Characteristics.RefundWarehouse,
            false,
            false);

        var (dcTypeValid, dcTypeMessage) =
            WarehouseValidationHelper.CheckCharacteristics(warehouseCharacteristics, request.TypeId!.Value);
        if (!dcTypeValid)
            throw new RpcException(new Status(StatusCode.InvalidArgument, dcTypeMessage!), dcTypeMessage!);

        var cancellationToken = context.CancellationToken;
        var warehouse = await _warehouseRepository.GetAsync((WarehouseId)request.WarehouseId, cancellationToken);

        var hasRegionsPriorities = await _warehousePrioritiesRepository.ListAsync(
            Array.Empty<RegionId>(),
            new[] { warehouse.WarehouseId },
            cancellationToken).AnyAsync(cancellationToken);

        if (hasRegionsPriorities && warehouse.TypeId != request.TypeId)
            throw new RpcException(
                new Status(StatusCode.InvalidArgument, "Cannot change the type of warehouse that has regions priorities."));

        var updatedCharacteristics = warehouseCharacteristics with
        {
            AutoReplenishment = warehouse.Characteristics.AutoReplenishment,
            Closed = warehouse.Characteristics.Closed
        };

        var requestModel = new Warehouse(
            (WarehouseId)request.WarehouseId,
            request.Name ?? warehouse.Name,
            warehouse.RezonId,
            warehouse.MetazonId,
            request.Address.RemoveExtraSpace(" "),
            string.IsNullOrWhiteSpace(request.Gln)
                ? warehouse.GLN
                : request.Gln.Trim(),
            request.TypeId ?? warehouse.TypeId,
            updatedCharacteristics,
            warehouse.GoodzonId,
            warehouse.NewType,
            warehouse.Assignment,
            warehouse.ManagementSystem);

        using var transactionScope = new TransactionScope(
            TransactionScopeOption.Required,
            new TransactionOptions { IsolationLevel = IsolationLevel.ReadCommitted },
            TransactionScopeAsyncFlowOption.Enabled);

        await _warehouseRepository.UpdateAsync(requestModel, context.GetPreferredUsername(), context.CancellationToken);

        if (request.RegionId != null)
            await _regionHomeWarehousesRepository.UpsertAsync(
                new[] { new RegionWarehouse((RegionId)request.RegionId.Value, (WarehouseId)request.WarehouseId) },
                context.GetPreferredUsername(),
                context.CancellationToken);

        if (ShouldSynchronize(request, warehouse))
            await _warehouseRepository.MarkAsShouldSynchronizeAsync(warehouse.WarehouseId, cancellationToken);

        transactionScope.Complete();

        return new UpdateWarehouseResponse();

        static bool ShouldSynchronize(UpdateWarehouseRequest request, Warehouse warehouse) =>
            request.Name != warehouse.Name
            || request.TypeId != warehouse.TypeId
            || (!string.IsNullOrEmpty(request.Gln) && request.Gln.Trim() != warehouse.GLN)
            || request.Address.RemoveExtraSpace(" ") != warehouse.Address
            || CharacteristicsChanged(request.Characteristics, warehouse.Characteristics);

        static bool CharacteristicsChanged(
            ProtoWarehouseCharacteristics protoCharacteristics,
            WarehouseCharacteristics characteristics) =>
            protoCharacteristics.BarcodeOnly != characteristics.BarcodeOnly ||
            protoCharacteristics.WmsSystem != characteristics.WmsSystem ||
            protoCharacteristics.TransitWarehouse != characteristics.TransitWarehouse ||
            protoCharacteristics.RefundWarehouse != characteristics.RefundWarehouse;
    }

    public override async Task<ListSaleRegionsResponse> ListSaleRegions(ListSaleRegionsRequest request, ServerCallContext context)
    {
        var warehouseSaleRegions = await _warehouseRepository
            .GetSaleRegionsAsync(context.CancellationToken)
            .ToArrayAsync(context.CancellationToken);
        return _mapper.Map<ListSaleRegionsResponse>(warehouseSaleRegions);
    }

    private async Task<WarehouseValidationResult> CheckDuplicateRecordsAsync(Warehouse warehouse, CancellationToken cancellationToken)
    {
        var warehouses = await _warehouseRepository.GetFilteredListAsync(
            warehouse.WarehouseId,
            warehouse.Name,
            warehouse.RezonId,
            warehouse.MetazonId,
            cancellationToken).ToArrayAsync(cancellationToken);

        if (warehouses.Any(w => w.WarehouseId == warehouse.WarehouseId))
            return new WarehouseValidationResult(
                false,
                $"Существует склад с ClearingId: {warehouse.WarehouseId}.");

        if (warehouses.Any(w => w.Name == warehouse.Name))
            return new WarehouseValidationResult(
                false,
                $"Существует склад с Названием: \"{warehouse.Name}\".");

        if (warehouses.Any(w => w.RezonId == warehouse.RezonId))
            return new WarehouseValidationResult(
                false,
                $"Существует склад с RezonId: {warehouse.RezonId}.");

        if (warehouses.Any(w => w.MetazonId == warehouse.MetazonId))
            return new WarehouseValidationResult(
                false,
                $"Существует склад с MetazonId: {warehouse.MetazonId}.");

        return new WarehouseValidationResult(true);
    }

    public override async Task<ListWarehousesResponse> ListAutoReplenishmentWarehouses(
        ListAutoReplenishmentWarehousesRequest request,
        ServerCallContext context)
    {
        var warehouses = await _warehouseRepository.GetAsync(
                new[] { new WarehouseTypeId(WarehouseTypes.AutoReplenishment) },
                null,
                context.CancellationToken)
            .ToArrayAsync(context.CancellationToken);

        return _mapper.Map<IEnumerable<Warehouse>, ListWarehousesResponse>(warehouses);
    }

    private async Task<WarehouseValidationResult> CheckNameDuplicatedAsync(
        WarehouseId warehouseId,
        string warehouseName,
        CancellationToken cancellationToken)
    {
        var isNamesDuplicated = await _warehouseRepository.IsNameDuplicatedAsync(warehouseId, warehouseName, cancellationToken);

        return isNamesDuplicated
            ? new WarehouseValidationResult(false, $"Существует склад с Названием: \"{warehouseName}\".")
            : new WarehouseValidationResult(true);
    }

    private async Task<WarehouseValidationResult> ValidateGlnAsync(
        long warehouseId,
        string address,
        string gln,
        CancellationToken cancellationToken)
    {
        if (!WarehouseValidationHelper.CheckGln(gln))
            return new WarehouseValidationResult(false, $"Значение поля GLN: {gln} введено неверно.");

        var addresses = await _warehouseRepository
            .GetWarehousesAddressesAsync((WarehouseId)warehouseId, gln, cancellationToken)
            .ToArrayAsync(cancellationToken);

        if (addresses.Length == 0)
            return new WarehouseValidationResult(true);

        var preparedAddress = address.RemoveExtraSpace("");

        return addresses
            .All(
                a => a.Address.RemoveExtraSpace("")
                    .Equals(preparedAddress, StringComparison.InvariantCultureIgnoreCase))
            ? new WarehouseValidationResult(true)
            : new WarehouseValidationResult(false, $"Существует склад с GLN: {gln}.");
    }

    public override async Task<ListWarehousesDetailsResponse> ListWarehousesDetails(
        ListWarehousesDetailsRequest request,
        ServerCallContext context)
    {
        var warehouses = await _warehouseRepository.GetAsync(
                request.WarehouseIds.Select(id => (WarehouseId)id).ToArray(),
                context.CancellationToken)
            .ToArrayAsync(context.CancellationToken);

        var warehousesDetails = GetWarehousesDetailsAsync(warehouses).ToArrayAsync(context.CancellationToken);

        return _mapper.Map<IEnumerable<WarehouseDetails>, ListWarehousesDetailsResponse>(warehousesDetails.Result);
    }

    private async IAsyncEnumerable<WarehouseDetails> GetWarehousesDetailsAsync(IEnumerable<Warehouse> warehouses)
    {
        var expressIds = await _whcWarehousesService.GetExpressWarehouseIdsAsync();
        foreach (var warehouse in warehouses)
            yield return
                new WarehouseDetails(warehouse, new WarehouseAttributes(expressIds.Contains(warehouse.WarehouseId)));
    }
}
