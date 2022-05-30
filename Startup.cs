using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Npgsql.TypeMapping;
using Ozon.Dvs.Wasabi.Protos;
using Ozon.Gdz.Platform.Auth;
using Ozon.Gdz.Platform.Grpc;
using Ozon.Gdz.Platform.Hosting;
using Ozon.Gdz.Platform.Mapping;
using Ozon.Gdz.RetailWorkspace.Contracts;
using Ozon.Gdz.Warehouses.Contracts;
using Ozon.Gdz.Warehouses.Controllers;
using Ozon.Gdz.Warehouses.Extensions;
using Ozon.Gdz.Warehouses.Mapping;
using Ozon.Gdz.Warehouses.OutboundPriorities;
using Ozon.Gdz.Warehouses.Persistence;
using Ozon.Gdz.Warehouses.Repository;
using Ozon.Gdz.Warehouses.WhcWarehousesProvider;
using Ozon.Platform.Data.Abstractions;
using Ozon.Platform.Data.Postgres;
using Ozon.Whc.Api.Warehouse.V2;

namespace Ozon.Gdz.Warehouses;

public class Startup
{
    private readonly IConfiguration _configuration;

    public Startup(IConfiguration configuration) => _configuration = configuration;

    public void ConfigureServices(IServiceCollection services)
    {
        // API
        services
            .AddAuthorization()
            .AddAuth(_configuration.GetSection("Auth").Get<AuthOptions>())
            .AddLogging(builder => builder.AddEventCounting())
            .AddAutoMapper(typeof(WarehouseMappingProfile).Assembly)
            .AddTransactionalBehavior()
            .AddPlatformGrpc(
                options =>
                {
                    options.EnableDetailedErrors = true;
                    options.AddExceptionHandler();
                });

        // Serialization
        services
            .AddJsonOptions(options => options.JsonSerializerOptions.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)
            .AddKafkaJsonSerializer<DvsEvent>();

        // Migrations
        services
            .AddFluentMigrator(typeof(InitialMigration).Assembly)
            .Configure<IPostgresConnectionFactory>((options, factory) => options.ConnectionString = factory.GetMasterConnectionString());

        // Persistence
        NpgsqlConnection.GlobalTypeMapper
            .MapIdentifiers<CustomIdentifierTypeHandler>(CustomIdentifierTypeHandler.SupportedTypes);
        services
            .AddPostgresDb("gdz-api-warehouses");

        // Configs
        services
            .AddOptions<GoodzonDbOptions>()
            .Configure<IConfiguration>(
                (options, configuration) =>
                    options.ConnectionString = configuration.BuildConnectionStringWithCredentials("Goodzon", "GoodzonDb"))
            .Services
            .AddOptions<WarehouseSynchronizerOptions>()
            .BindConfiguration(nameof(WarehouseSynchronizerOptions))
            .Services
            .AddOptions<OutboundPrioritiesServiceOptions>()
            .Configure<IConfiguration, IHostEnvironment>(
                (options, configuration, environment) =>
                {
                    options.Host = configuration.GetHostOrThrow("Kafka");
                    options.Group = environment.ApplicationName;
                })
            .BindConfiguration(nameof(OutboundPrioritiesServiceOptions))
            .Services
            .AddOptions<PrioritiesNotificationsServiceOptions>()
            .BindConfiguration(nameof(PrioritiesNotificationsServiceOptions));

        // Metrics
        services
            .AddSingleton<NonSynchronizedWarehousesMetric>()
            .AddOptions<WarehousesMetricCollectorOptions>()
            .BindConfiguration(nameof(WarehousesMetricCollectorOptions));

        // Repositories
        services
            .AddScoped<IWarehouseRepository, WarehouseRepository>()
            .AddScoped<IWarehousePrioritiesRepository, WarehousePrioritiesRepository>()
            .AddScoped<IWarehouseTypeRepository, WarehouseTypeRepository>()
            .AddScoped<IRegionRepository, RegionRepository>()
            .AddScoped<IRegionHomeWarehousesRepository, RegionHomeWarehousesRepository>()
            .AddScoped<IDistributionCenterPrioritiesRepository, DistributionCenterPrioritiesRepository>()
            .AddScoped<IWarehouseOutboundPrioritiesRepository, WarehouseOutboundPrioritiesRepository>();

        // Services
        services
            .AddHostedService<WarehouseSynchronizer>()
            .AddHostedService<WarehousesMetricCollector>()
            .AddHostedService<OutboundPrioritiesService>()
            .AddHostedService<PrioritiesNotificationsService>()
            .AddScoped<IGoodzonManager, GoodzonManager>()
            .AddScoped<IRegionUpdater, RegionUpdater>()
            .AddScoped<IWhcWarehousesService, WhcWarehousesService>();

        // External
        services
            .RegisterGrpcClient<WarehouseApi.WarehouseApiClient>("whc-go-api-warehouse", _configuration)
            .AddGrpcRetryPolicy(3)
            .Services
            .RegisterGrpcClient<WarehouseHandlers.WarehouseHandlersClient>("dvs-wasabi", _configuration)
            .AddGrpcRetryPolicy(3)
            .Services
            .RegisterGrpcClient<NotificationsService.NotificationsServiceClient>("gdz-api-retail-workspace", _configuration);
    }

    public static void Configure(IApplicationBuilder app, IWebHostEnvironment env) =>
        app
            .ValidateAutoMapperConfiguration()
            .UseRouting()
            .UseAuthentication()
            .UseAuthorization()
            .UseEndpoints(
                endpoints =>
                {
                    endpoints.MapGrpcService<WarehousesGrpcController>();
                    endpoints.MapGrpcService<RegionsGrpcController>();
                    endpoints.MapGrpcService<WarehousePriorityGrpcController>();
                    endpoints.MapGrpcService<RegionHomeWarehousesGrpcController>();
                    endpoints.MapGrpcService<DistributionCenterPriorityGrpcController>();
                    endpoints.MapGrpcService<WarehouseOutboundPriorityGrpcController>();
                    endpoints.MapPlatformServices();
                });
}
