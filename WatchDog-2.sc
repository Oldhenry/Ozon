using Instruments.Observability.SearchApi.Observability.Logging;

using Instruments.Observability.SearchApi.Observability.Logging.Models;

using Quartz;

using SearchApi.ConfigurationService.Models.Interfaces;

using SearchApi.ConfigurationService.Models.Models;

using SearchApi.ConfigurationService.WebApi.Helpers;

using SearchApi.ModelsAndInterfaces.Models;

 

namespace SearchApi.ConfigurationService.WebApi.Services

{

    /// Класс для работы в фоновом режиме в сервисе конфигурации <inheritdoc cref="IWatchDog"/>

    [DisallowConcurrentExecution]

    public class WatchDog : IWatchDog

    {

        private readonly IGitAccessor _gitAccessor;

        private readonly IConfigurationRegistryDataReader _dataReader;

        private readonly IConfigurationRegistryDataWriter _dataWriter;

        private readonly IConfigurationServiceConstantsProvider _constantsProvider;

        private readonly ArchiveHelper _archiveHelper;

        private readonly ILoggerWrapper<WatchDog> _logger;

        private readonly string _dbSchema;

        private readonly int _timeout;

 

        /// <summary>

        /// Конструктор с зависимостями

        /// </summary>

        public WatchDog(

            DbConfiguration dbConfiguration,

            IGitAccessor gitAccessor,

            IConfigurationRegistryDataReader dataReader,

            IConfigurationRegistryDataWriter dataWriter,

            IConfigurationServiceConstantsProvider constantsProvider,

            ArchiveHelper archiveHelper,

            ILoggerWrapper<WatchDog> logger)

        {

            _gitAccessor = gitAccessor;

            _dataReader = dataReader;

            _dataWriter = dataWriter;

            _constantsProvider = constantsProvider;

            _archiveHelper = archiveHelper;

            _logger = logger;

           

            _dbSchema = dbConfiguration.DefaultSchema;

            _timeout = dbConfiguration.QueryTimeoutInSeconds;

        }

 

        /// <summary>

        /// Фоновый процесс на основе библиотеки Quartz

        /// </summary>

        public async Task Execute(IJobExecutionContext context)

        {

            _logger.LogInformation(

                $"WatchDog started at {DateTime.Now}",

                LogDataType.Logs,

                LogType.Action);

           

           await ExecuteAsync(context.CancellationToken);

          

            _logger.LogInformation(

                $"WatchDog finished at {DateTime.Now}",

                LogDataType.Logs,

                LogType.Action);

        }

     

        /// <inheritdoc cref="IWatchDog.ExecuteAsync"/>

        public async Task<ICollection<ConfigurationRegistryRecord>> ExecuteAsync(CancellationToken cancellationToken)

        {

            _logger.LogInformation(

                $"Start cloning repository at {DateTime.Now}",

                LogDataType.Logs,

                LogType.Action);

 

            var configRegistryRecords = new Dictionary<int, ConfigurationRegistryRecord>();

 

            while (true)

            {

                var dbRecord =

                    await _dataReader.GetCheckedConfigurationRegistryAsync(configRegistryRecords.Keys.ToArray(),

                        _dbSchema, _timeout, default);

                if (dbRecord == null)

                {

                    break;

                }

 

                configRegistryRecords.Add(dbRecord.Id, dbRecord);

 

                try

                {

                    using (var tokenSource =

                           new CancellationTokenSource(TimeSpan.FromMinutes(dbRecord.RepoTimeoutMinutes)))

                    {

                        if (_gitAccessor.IsLocalRepositoryValid(dbRecord.KeCode, dbRecord.RepoUrl).Status ==

                            RepositoryStatus.NotExists)

                        {

                            await GetRepositoryAsync(dbRecord, tokenSource.Token);

                        }

 

                        var branchInfo = await GetBranchInfoAsync(dbRecord, tokenSource.Token);

 

                        if (branchInfo.IsNeedUpdate && !tokenSource.IsCancellationRequested)

                        {

                            await UpdateRecordAsync(dbRecord, branchInfo, tokenSource.Token);

                        }

                    }

                }

                catch (Exception ex)

                {

                    _logger.LogError(

                        LogExCode.Internal,

                        ex is OperationCanceledException

                            ? _constantsProvider.OperationCanceledError(ex.Message)

                            : _constantsProvider.WatchDogError(ex.Message),

                        LogDataType.Logs,

                        LogType.Action);

 

                    configRegistryRecords.Remove(dbRecord.Id);

                }

 

                try

                {

                    await _dataWriter.UpdateInProcessAsync(dbRecord, _dbSchema, _timeout, CancellationToken.None);

                }

                catch (Exception ex)

                {

                    _logger.LogError(

                        LogExCode.Internal,

                        _constantsProvider.WatchDogUpdateRecordError(dbRecord, ex.Message),

                        LogDataType.Logs,

                        LogType.Action);

                }

            }

 

            var result = configRegistryRecords.Values.Select(r => new ConfigurationRegistryRecord

                {

                    Id = r.Id,

                    BranchName = r.BranchName,

                    KeCode = r.KeCode,

                    CheckPeriodMinutes = r.CheckPeriodMinutes,

                    RepoTimeoutMinutes = r.RepoTimeoutMinutes,

                    LastCheckTime = r.LastCheckTime,

                    CommitHash = r.CommitHash,

                   CommitDate = r.CommitDate,

                    CommitAuthor = r.CommitAuthor,

                    CommitMessage = r.CommitMessage

                }

            ).ToArray();

 

            foreach (var record in result)

            {

                _logger.LogInformation(

                    $"Result info: {_constantsProvider.ConfigurationRegistryRecordInfoMessage(record)}",

                    LogDataType.Logs,

                    LogType.Action);

            }

 

            _logger.LogInformation(

                $"Stop cloning repository at {DateTime.Now}",

                LogDataType.Logs,

                LogType.Action);

 

            return result;

        }

 

        /// <summary>

        /// Клонирование репозитория в локальную папку

        /// </summary>

        private async Task GetRepositoryAsync(ConfigurationRegistryRecord dbRecord, CancellationToken token)

        {

            if (string.IsNullOrEmpty(dbRecord.CommitHash))

            {

                _logger.LogInformation(

                    $"Clone repository: {_constantsProvider.ConfigurationRegistryRecordInfoMessage(dbRecord)}",

                    LogDataType.Logs,

                    LogType.Action);

               

                await _gitAccessor.CloneRepositoryAsync(

                    dbRecord.KeCode,

                    dbRecord.RepoUrl,

                    dbRecord.BranchName,

                    token);

               

                token.ThrowIfCancellationRequested();

            }

            else

            {

                var recordDto = await _dataReader.GetConfigurationRegistryByIdAsync(dbRecord.Id, token);

                if (recordDto != null)

                {

                    _logger.LogInformation(

                        $"Extract repository: {_constantsProvider.ConfigurationRegistryRecordInfoMessage(recordDto)}",

                        LogDataType.Logs,

                        LogType.Action);

 

                    _archiveHelper.ExtractRepositoryFromArchive(recordDto);

                   

                    token.ThrowIfCancellationRequested();

                }

            }

        }

 

        /// <summary>

        /// Получение текущей ветки

        /// </summary>

        private async Task<BranchInfo> GetBranchInfoAsync(ConfigurationRegistryRecord configurationRecord, CancellationToken token)

        {

            var branchInfo = await _gitAccessor.CheckoutBranchAsync(

                configurationRecord.KeCode,

                configurationRecord.RepoUrl,

                configurationRecord.BranchName,

                token);

 

            token.ThrowIfCancellationRequested();

 

            if (branchInfo == null)

            {

                return null;

            }

 

            if (!string.IsNullOrEmpty(configurationRecord.CommitHash) && configurationRecord.CommitHash == branchInfo.CommitInfo.CommitHash)

            {

                return branchInfo;

            }

 

            branchInfo.IsNeedUpdate = true;

 

            return branchInfo;

        }

 

        /// <summary>

        /// Изменение записи в БД

        /// </summary>

        /// <param name="configurationRecord">Dto для изменения</param>

        /// <param name="branchInfo">Информация о ветке</param>

        /// <param name="cancellationToken">Токен отмены</param>

        private async Task UpdateRecordAsync(

            ConfigurationRegistryRecord configurationRecord,

            BranchInfo branchInfo,

            CancellationToken cancellationToken)

        {

            configurationRecord.CommitHash = branchInfo.CommitInfo.CommitHash;

            configurationRecord.CommitAuthor = $"{branchInfo.CommitInfo.UserName} <{branchInfo.CommitInfo.UserEmail}>";

            configurationRecord.CommitDate = branchInfo.CommitInfo.CommitDate.ToLocalTime();

            configurationRecord.CommitMessage = branchInfo.CommitInfo.Message;

            configurationRecord.RepoArchive = _archiveHelper.CreateArchive(configurationRecord.KeCode);

           

            await _dataWriter.UpdateConfigurationRegistryAsync(configurationRecord, _dbSchema, _timeout, cancellationToken);

        }

 

        /// <inheritdoc cref="IWatchDog.CheckConcurrencyAsync"/>

        public async Task<ICollection<ConfigurationRegistryRecord>> CheckConcurrencyAsync()

        {

            const int id17 = 17;

            const int id18 = 18;

           

            var list = new List<Task<ConfigurationRegistryRecord>>();

           

            // Check id = 17

            for (int i = 0; i < 5; i++)

            {

                list.Add(_dataReader.UpdateRecordConcurrencyAsync(id17, _dbSchema, _timeout, CancellationToken.None));

            }

            var result = (await Task.WhenAll(list)).ToList();

           

            // Check id = 18

            result.AddRange((await Task.WhenAll(

                _dataReader.UpdateRecordConcurrencyAsync(id18, _dbSchema, _timeout, CancellationToken.None),

                _dataReader.UpdateRecordConcurrencyAsync(id18, _dbSchema, _timeout, CancellationToken.None),

                _dataReader.UpdateRecordConcurrencyAsync(id18, _dbSchema, _timeout, CancellationToken.None)

            )).ToList());

           

            // Update inProcess = false

            await _dataWriter.UpdateInProcessAsync(

                new ConfigurationRegistryRecord() { Id = id17 },

                _dbSchema,

                _timeout,

                CancellationToken.None);

            await _dataWriter.UpdateInProcessAsync(

                new ConfigurationRegistryRecord() { Id =id18 },

                _dbSchema,

                _timeout,

                CancellationToken.None);

 

            return result;

        }

    }

}

 

using Instruments.Observability.SearchApi.Observability.Logging;

using Instruments.Observability.SearchApi.Observability.Logging.Models;

using LibGit2Sharp;

using SearchApi.ConfigurationService.Models.Exceptions;

using SearchApi.ConfigurationService.Models.Interfaces;

using SearchApi.ConfigurationService.Models.Models;

using SearchApi.ConfigurationService.WebApi.Helpers;

using RepositoryStatus = SearchApi.ConfigurationService.Models.Models.RepositoryStatus;

 

namespace SearchApi.ConfigurationService.WebApi.Services

{

    /// Класс для работы с Git <inheritdoc cref="IGitAccessor"/>

    public class GitAccessor : IGitAccessor

    {

        private const string Origin = "origin";

        private const string KeCode ="KeCode";

        private const string RepositoryUrl = "RepositoryUrl";

 

        private const string Clone = "clone";

        private const string Fetch = "fetch";

        private const string Pull = "pull";

 

        private readonly IConfigurationServiceConstantsProvider _constantsProvider;

        private readonly FileHelper _fileHelper;

        private readonly string _localRepositoryDirectory;

        private readonly ILoggerWrapper<GitAccessor> _logger;

 

        /// <summary>

        /// Конструктор с зависимостями

        /// </summary>

        public GitAccessor(

            ConfigurationServiceParameters configurationServiceParameters,

            IConfigurationServiceConstantsProvider constantsProvider,

            FileHelper fileHelper,

            ILoggerWrapper<GitAccessor> logger)

        {

            _localRepositoryDirectory = configurationServiceParameters.ConfigurationRepositoriesDirectory;

            _constantsProvider = constantsProvider;

            _fileHelper = fileHelper;

           _logger = logger;

        }

 

        /// <inheritdoc cref="IGitAccessor.IsLocalRepositoryValid"/>

        public RepositoryInfo IsLocalRepositoryValid(string keCode, string repoUrl) =>

            Repository.IsValid(Path.Combine(_localRepositoryDirectory, keCode))

                ? new RepositoryInfo(RepositoryStatus.Exists, keCode, repoUrl)

                : new RepositoryInfo(RepositoryStatus.NotExists, keCode, repoUrl);

 

        /// <inheritdoc cref="IGitAccessor.CloneRepositoryAsync"/>

        public async Task<RepositoryInfo> CloneRepositoryAsync(

            string keCode,

            string repoUrl,

            string branchName,

            CancellationToken token)

        {

            var errorMessage = $"{KeCode}: {keCode}, {RepositoryUrl}: {repoUrl}";

            var localRepositoryDirectoryName = Path.Combine(_localRepositoryDirectory, keCode);

 

            _fileHelper.PrepareTargetDirectory(localRepositoryDirectoryName);

 

            try

            {

                var processHelper = new ProcessHelper(repoUrl, localRepositoryDirectoryName, branchName);

 

                _logger.LogInformation(

                    _constantsProvider.GitOperationInfoMessage(Clone, keCode, branchName),

                    LogDataType.Logs,

                    LogType.Action);

 

                var processResult = await processHelper.CloneAsync(token);

 

                token.ThrowIfCancellationRequested();

               

                if (processResult.ExitCode != 0)

                {

                    throw new ConfigurationServiceException(_constantsProvider.ProcessError(processResult.StandardError), null);

                }

 

                return new RepositoryInfo(RepositoryStatus.Cloned, keCode, repoUrl);

            }

            catch (OperationCanceledException ex)

            {

                throw new ConfigurationServiceException(_constantsProvider.OperationCanceledError(errorMessage), ex);

            }

            catch (Exception ex)

            {

                throw new ConfigurationServiceException(_constantsProvider.RepositoryNotFoundError(errorMessage), ex);

            }

        }

 

        /// <inheritdoc cref="IGitAccessor.CheckoutBranchAsync"/>

        public async Task<BranchInfo> CheckoutBranchAsync(string keCode, string repoUrl, string branchName, CancellationToken token)

        {

            if (token.IsCancellationRequested)

            {

                return null;

            }

            

            var errorMessage = $"{KeCode}: {keCode}, {RepositoryUrl}: {repoUrl}";

            var localRepositoryDirectoryName = Path.Combine(_localRepositoryDirectory, keCode);

            var processHelper = new ProcessHelper(repoUrl, localRepositoryDirectoryName, branchName);

          

            _logger.LogInformation(

                _constantsProvider.GitOperationInfoMessage(Fetch, keCode, branchName),

                LogDataType.Logs,

                LogType.Action);

 

            try

            {

                var processResult = await processHelper.FetchAsync(token);

 

                token.ThrowIfCancellationRequested();

 

                if (processResult.ExitCode != 0)

                {

                    throw new ConfigurationServiceException(

                        _constantsProvider.ProcessError(processResult.StandardError), null);

                }

 

                using (var repo = new Repository(localRepositoryDirectoryName))

                {

                    token.ThrowIfCancellationRequested();

 

                    var localBranch = await VerifyLocalBranch(repo, keCode, branchName, processHelper, token);

                    if (localBranch == null)

                    {

                        throw new ConfigurationServiceException(_constantsProvider.BranchNotFoundError(branchName),

                            null);

                    }

 

                    return Commands

                        .Checkout(repo, localBranch,

                            new CheckoutOptions { CheckoutModifiers = CheckoutModifiers.Force })

                        .MapBranch(keCode);

                }

            }

            catch (OperationCanceledException ex)

            {

                throw new ConfigurationServiceException(_constantsProvider.OperationCanceledError(errorMessage), ex);

            }

            catch (Exception ex)

            {

                throw new ConfigurationServiceException(_constantsProvider.RepositoryNotFoundError(errorMessage), ex);

            }

        }

 

        /// <summary>

        /// Проверка локальной ветки

        /// </summary>

        private async Task<Branch> VerifyLocalBranch(IRepository repo, string keCode, string branchName, ProcessHelper processHelper, CancellationToken token)

        {

            if (token.IsCancellationRequested)

            {

                return null;

            }

           

            var remoteBranch = repo.Branches[$"{Origin}/{branchName}"];

            if (remoteBranch == null)

            {

                throw new ConfigurationServiceException(_constantsProvider.BranchNotFoundError(branchName), null);

            }

 

            var localBranch = repo.Branches[branchName];

            if (localBranch == null)

            {

                throw new ConfigurationServiceException(_constantsProvider.BranchNotFoundError(branchName), null);

            }

 

            if (localBranch.Tip.Sha.Equals(remoteBranch.Tip.Sha, StringComparison.InvariantCultureIgnoreCase))

            {

                return repo.Branches[branchName];

            }           

 

            _logger.LogInformation(

                _constantsProvider.GitOperationInfoMessage(Pull, keCode, branchName),

                LogDataType.Logs,

                LogType.Action);

 

            var processResult = await processHelper.PullAsync(token);

          

            token.ThrowIfCancellationRequested();

 

            if (processResult.ExitCode != 0)

            {

                throw new ConfigurationServiceException(_constantsProvider.ProcessError(processResult.StandardError), null);

            }

 

            return repo.Branches[branchName];

        }

    }

}

 

using Quartz;

using SearchApi.ConfigurationService.WebApi.Services;

 

namespace SearchApi.ConfigurationService.WebApi.RegisterDependencies

{

    public static class RegisterQuartzExtensions

    {

        private const string JobKey = "WatchDogJobKey";

        private const string JobTrigger = "WatchDogJobTrigger";

        private const string CronSchedule = "QuartzCronSchedule";

 

        /// <summary>

        /// Регистрация Quartz сервиса

        /// </summary>

        public static WebApplicationBuilder RegisterQuartz(this WebApplicationBuilder builder)

        {

            var cronSchedule = builder.Configuration.GetSection(CronSchedule).Get<string>();

           

            builder.Services.AddQuartz(q =>

            {

                var jobKey = new JobKey(JobKey);

       

                q.UseMicrosoftDependencyInjectionJobFactory();

 

                q.AddJob<WatchDog>(opts => opts.WithIdentity(jobKey));

 

                q.AddTrigger(opts => opts

                    .ForJob(jobKey)

                    .WithIdentity(JobTrigger)

                    .WithCronSchedule(cronSchedule));

            });

           

            builder.Services.AddQuartzHostedService(q => q.WaitForJobsToComplete = true);

            return builder;

        }

    }

}

 

 

using System.Data;
using System.Data.Common;
using Instruments.Observability.SearchApi.Observability.Logging;
using Instruments.Observability.SearchApi.Observability.Logging.Models;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using NpgsqlTypes;
using SearchApi.ConfigurationService.Models.Exceptions;
using SearchApi.ConfigurationService.Models.Interfaces;
using SearchApi.ConfigurationService.Models.Models;
using SearchApi.ConfigurationService.WebApi.Helpers;
using SearchApi.TF;

namespace SearchApi.ConfigurationService.WebApi.DbContextAccessors
{
    /// <summary>
    /// Имплементация <see cref="IConfigurationRegistryDataReader"/>
    /// </summary>
    public class ConfigurationRegistryDataReader : IConfigurationRegistryDataReader
    {
        private readonly IContextFlowFactory<SearchApiDbContext.SearchApiDbContext> _contextFactory;
        private readonly IConfigurationServiceConstantsProvider _configurationServiceConstantsProvider;
        private readonly ILoggerWrapper<ConfigurationRegistryDataReader> _logger;
       
        /// <summary>
        /// Конструктор с базовыми зависимостями
        /// </summary>
        public ConfigurationRegistryDataReader(
            IContextFlowFactory<SearchApiDbContext.SearchApiDbContext> contextFactory,
            IConfigurationServiceConstantsProvider configurationServiceConstantsProvider,
            ILoggerWrapper<ConfigurationRegistryDataReader> logger)
        {
            _contextFactory = contextFactory;
            _configurationServiceConstantsProvider = configurationServiceConstantsProvider;
            _logger = logger;
        }

        /// <inheritdoc cref="IConfigurationRegistryDataReader.GetCheckedConfigurationRegistryAsync"/>
        public async Task<ConfigurationRegistryRecord> GetCheckedConfigurationRegistryAsync(
            ICollection<int> excludeIds,
            string dbSchema,
            int commandTimeout,
            CancellationToken cancellationToken)
        {
            try
            {
                using (var context = await _contextFactory.GetContextValueAsync(cancellationToken))
                {
                    using (var connection = context.Value.GetDbConnectionWrapper())
                    {
                        await connection.OpenAsync(cancellationToken);
                       
                        await using (var command = connection.CreateCommand(SqlHelper.UpdateAndGetSelectOneRecord(dbSchema), new DbParameter[]
                                     {
                                         new NpgsqlParameter("excludeIds", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = excludeIds }
                                     }))
                        {
                            command.CommandType = CommandType.Text;
                            command.CommandTimeout = commandTimeout;

                            await using (var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow, cancellationToken))
                            {
                                if (!reader.HasRows)
                                {
                                    return null;
                                }

                                while (await reader.ReadAsync(cancellationToken))
                                {
                                    return new ConfigurationRegistryRecord()
                                    {
                                        Id = reader.GetFieldValue<int>(0),
                                        KeCode = reader.GetFieldValue<string>(1),
                                        RepoUrl = reader.GetFieldValue<string>(2),
                                        BranchName = reader.GetFieldValue<string>(3),
                                        InProcess = reader.GetFieldValue<bool>(4),
                                        CheckPeriodMinutes = reader.GetFieldValue<int>(5),
                                        RepoTimeoutMinutes = reader.GetFieldValue<int>(6),
                                        CommitHash = reader.IsDBNull(8) ? null : reader.GetFieldValue<string>(8)
                                    };
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    LogExCode.Internal,
                    _configurationServiceConstantsProvider.DataBaseError,
                    LogDataType.Logs,
                    LogType.Action);

                throw new ConfigurationServiceException(_configurationServiceConstantsProvider.DataBaseError, ex);
            }

            return null;
        }

        /// <inheritdoc cref="IConfigurationRegistryDataReader.GetConfigurationRegistryByIdAsync"/>
        public async Task<ConfigurationRegistryRecord> GetConfigurationRegistryByIdAsync(int id, CancellationToken cancellationToken)
        {
            try
            {
                using (var context = await _contextFactory.GetContextValueAsync(cancellationToken))
                {
                    var dbRecord = await context.Value
                        .ConfigurationRegistry
                        .AsNoTracking()
                        .FirstOrDefaultAsync(r => r.Id == id, cancellationToken);

                    return dbRecord != null
                        ? new ConfigurationRegistryRecord()
                        {
                            Id = dbRecord.Id,
                            KeCode = dbRecord.KeCode,
                            RepoUrl = dbRecord.RepoUrl,
                            BranchName = dbRecord.BranchName,
                            InProcess = dbRecord.InProcess,
                            CheckPeriodMinutes = dbRecord.CheckPeriodMinutes,
                            RepoTimeoutMinutes = dbRecord.RepoTimeoutMinutes,
                            LastCheckTime = dbRecord.LastCheckTime,
                            CommitHash = dbRecord.CommitHash,
                            CommitAuthor = dbRecord.CommitAuthor,
                            RepoArchive = dbRecord.RepoArchive,
                            CommitDate = dbRecord.CommitDate,
                            CommitMessage = dbRecord.CommitMessage
                        }
                        : null;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    LogExCode.Internal,
                    _configurationServiceConstantsProvider.DataBaseError,
                    LogDataType.Logs,
                    LogType.Action);

                throw new ConfigurationServiceException(_configurationServiceConstantsProvider.DataBaseError, ex);
            }
        }
       
         /// <inheritdoc cref="IConfigurationRegistryDataReader.UpdateRecordConcurrencyAsync"/>
        public async Task<ConfigurationRegistryRecord> UpdateRecordConcurrencyAsync(int id, string dbSchema, int commandTimeout, CancellationToken cancellationToken)
        {
            try
            {
                using (var context = await _contextFactory.GetContextValueAsync(cancellationToken))
                {
                    using (var connection = context.Value.GetDbConnectionWrapper())
                    {
                        await connection.OpenAsync(cancellationToken);
                       
                        await using (var command = connection.CreateCommand(SqlHelper.UpdateOneRecordConcurrency(dbSchema), new DbParameter[]
                                     {
                                         new NpgsqlParameter("id", NpgsqlDbType.Integer) { Value = id }
                                     }))
                        {
                            command.CommandType = CommandType.Text;
                            command.CommandTimeout = commandTimeout;

                            await using (var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow, cancellationToken))
                            {
                                if (!reader.HasRows)
                                {
                                    return null;
                                }

                                while (await reader.ReadAsync(cancellationToken))
                                {
                                    return new ConfigurationRegistryRecord()
                                    {
                                        Id = reader.GetFieldValue<int>(0),
                                        KeCode = reader.GetFieldValue<string>(1),
                                        RepoUrl = reader.GetFieldValue<string>(2),
                                        BranchName = reader.GetFieldValue<string>(3),
                                        InProcess = reader.GetFieldValue<bool>(4),
                                        CheckPeriodMinutes = reader.GetFieldValue<int>(5),
                                        RepoTimeoutMinutes = reader.GetFieldValue<int>(6),
                                        CommitHash = reader.IsDBNull(8) ? null : reader.GetFieldValue<string>(8),
                                        CommitAuthor = reader.IsDBNull(10) ? null : reader.GetFieldValue<string>(10),
                                        CommitDate = reader.IsDBNull(11) ? null : reader.GetFieldValue<DateTime?>(11),
                                        CommitMessage = reader.IsDBNull(11) ? null : reader.GetFieldValue<string>(12),
                                        LastCheckTime = reader.IsDBNull(8) ? null : reader.GetFieldValue<DateTime?>(7)
                                    };
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    LogExCode.Internal,
                    _configurationServiceConstantsProvider.DataBaseError,
                    LogDataType.Logs,
                    LogType.Action);

                throw new ConfigurationServiceException(_configurationServiceConstantsProvider.DataBaseError, ex);
            }

            return null;
        }
    }
}

 

 

using System.Data;

using System.Data.Common;

using Instruments.Observability.SearchApi.Observability.Logging;

using Instruments.Observability.SearchApi.Observability.Logging.Models;

using Npgsql;

using NpgsqlTypes;

using SearchApi.ConfigurationService.Models.Exceptions;

using SearchApi.ConfigurationService.Models.Interfaces;

using SearchApi.ConfigurationService.Models.Models;

using SearchApi.ConfigurationService.WebApi.Helpers;

using SearchApi.ModelsAndInterfaces.Interfaces.Serialization;

using SearchApi.TF;

 

namespace SearchApi.ConfigurationService.WebApi.DbContextAccessors

{

    /// <summary>

    /// Имплементация <see cref="IConfigurationRegistryDataWriter"/>

    /// </summary>

    public class ConfigurationRegistryDataWriter : IConfigurationRegistryDataWriter

    {

        private readonly IContextFlowFactory<SearchApiDbContext.SearchApiDbContext> _contextFactory;

        private readonly IConfigurationServiceConstantsProvider _configurationServiceConstantsProvider;

        private readonly ILoggerWrapper<ConfigurationRegistryDataWriter> _logger;

        private readonly ISerializer _serializer;

       

        /// <summary>

        /// Конструктор с базовыми зависимостями

        /// </summary>

        public ConfigurationRegistryDataWriter(

            IContextFlowFactory<SearchApiDbContext.SearchApiDbContext> contextFactory,

            IConfigurationServiceConstantsProvider configurationServiceConstantsProvider,

            ISerializer serializer,

            ILoggerWrapper<ConfigurationRegistryDataWriter> logger)

        {

            _contextFactory = contextFactory;

            _configurationServiceConstantsProvider = configurationServiceConstantsProvider;

            _serializer = serializer;

            _logger = logger;

        }

       

        /// <inheritdoc cref="IConfigurationRegistryDataWriter.UpdateConfigurationRegistryAsync"/>

        public async Task UpdateConfigurationRegistryAsync(

            ConfigurationRegistryRecord configurationRecord,

            string dbSchema,

            int commandTimeout,

            CancellationToken cancellationToken)

        {

            try

            {

                _logger.LogDebug(

                    _configurationServiceConstantsProvider.DbWriteConfigurationRegistry(_serializer.Serialize(configurationRecord)),

                    LogDataType.Logs,

                    LogType.Action);

 

                using (var context = await _contextFactory.GetContextValueAsync(cancellationToken))

                {

                    using (var connection = context.Value.GetDbConnectionWrapper())

                    {

                        await connection.OpenAsync(cancellationToken);

                       

                        await using (var command = connection.CreateCommand(SqlHelper.GetUpdateQuery(dbSchema), new DbParameter[]

                                     {

                                         new NpgsqlParameter("commit_hash", NpgsqlDbType.Text) { Value = configurationRecord.CommitHash },

                                         new NpgsqlParameter("commit_author", NpgsqlDbType.Text) { Value = configurationRecord.CommitAuthor },

                                         new NpgsqlParameter("commit_date", NpgsqlDbType.TimestampTz) { Value = configurationRecord.CommitDate!.Value.ToUniversalTime() },

                                         new NpgsqlParameter("commit_message", NpgsqlDbType.Text) { Value = configurationRecord.CommitMessage },

                                         new NpgsqlParameter("repo_archive", NpgsqlDbType.Bytea) { Value = configurationRecord.RepoArchive },

                                         new NpgsqlParameter("id", NpgsqlDbType.Integer) { Value = configurationRecord.Id }

                                     }))                            

                        {

                            command.CommandType = CommandType.Text;

                            command.CommandTimeout = commandTimeout;

 

                            await command.ExecuteNonQueryAsync(cancellationToken);

                        }

                    }

                }

            }

            catch (Exception ex)

            {

                _logger.LogError(

                    ex,

                    LogExCode.Internal,

                    _configurationServiceConstantsProvider.DataBaseError,

                    LogDataType.Logs,

                    LogType.Action);

 

                throw new ConfigurationServiceException(_configurationServiceConstantsProvider.DataBaseError, ex);

            }

        }

      

        /// <inheritdoc cref="IConfigurationRegistryDataWriter.UpdateInProcessAsync"/>

        public async Task UpdateInProcessAsync(

            ConfigurationRegistryRecord configurationRecord,

            string dbSchema,

            int commandTimeout,

            CancellationToken cancellationToken)

        {

            try

            {

                _logger.LogDebug(

                    _configurationServiceConstantsProvider.DbWriteConfigurationRegistry(_serializer.Serialize(configurationRecord)),

                    LogDataType.Logs,

                    LogType.Action);

 

                using (var context = await _contextFactory.GetContextValueAsync(cancellationToken))

                {

                    using (var connection = context.Value.GetDbConnectionWrapper())

                    {

                        await connection.OpenAsync(cancellationToken);

 

                        await using (var command = connection.CreateCommand(SqlHelper.GetUpdateProcessQuery(dbSchema), new DbParameter[]

                                         {

                                             new NpgsqlParameter("id", NpgsqlDbType.Integer) { Value = configurationRecord.Id }

                                         }))

                        {

                            command.CommandTimeout = commandTimeout;

                            command.CommandType = CommandType.Text;

 

                            await command.ExecuteNonQueryAsync(cancellationToken);

                        }

                    }

                }

            }

            catch (Exception ex)

            {

                _logger.LogError(

                    ex,

                    LogExCode.Internal,

                    _configurationServiceConstantsProvider.DataBaseError,

                    LogDataType.Logs,

                    LogType.Action);

 

                throw new ConfigurationServiceException(_configurationServiceConstantsProvider.DataBaseError, ex);

            }

        }

    }

}
