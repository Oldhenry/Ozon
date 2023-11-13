namespace SearchApi.ConfigurationService.WebApi.Helpers

{

    /// <summary>

    /// Клас для построения и получения SQL запросов

    /// </summary>

    public static class SqlHelper

    {

        /// <summary>

        /// Запрос на именение записи в таблице configuration_registry

        /// </summary>

        /// <param name="dbSchema">Схема БД</param>

        /// <returns></returns>

        public static string GetUpdateQuery(string dbSchema) => $@"

        BEGIN WORK;

 

        LOCK TABLE {dbSchema}.configuration_registry IN ROW EXCLUSIVE MODE;

 

        SELECT * FROM {dbSchema}.configuration_registry WHERE id = :id FOR UPDATE;

      

        UPDATE {dbSchema}.configuration_registry

        SET

           last_check_time = CURRENT_TIMESTAMP,

           in_process = false,

           commit_hash = :commit_hash,

           repo_archive = :repo_archive,

           commit_author = :commit_author,

           commit_date = :commit_date,

           commit_message = :commit_message          

        WHERE id = :id;

       

        COMMIT WORK;";

 

        /// <summary>

        /// Запрос на именение полей {in_process, last_check_time} в таблице configuration_registry

        /// </summary>

        /// <param name="dbSchema">Схема БД</param>

        /// <returns></returns>

        public static string GetUpdateProcessQuery(string dbSchema) => $@"

        BEGIN WORK;

 

        LOCK TABLE {dbSchema}.configuration_registry IN ROW EXCLUSIVE MODE;

       

        SELECT * FROM {dbSchema}.configuration_registry WHERE id = :id FOR UPDATE;

   

        UPDATE  {dbSchema}.configuration_registry

        SET

        last_check_time = CURRENT_TIMESTAMP,

        in_process = false          

        WHERE id = :id;

 

        COMMIT WORK;";

 

        /// <summary>

        /// Запрос на именение и получение одной записи в таблице configuration_registry

        /// </summary>

        /// <param name="dbSchema">Схема БД</param>

        /// <returns></returns>

        public static string UpdateAndGetSelectOneRecord(string dbSchema) => $@"

        BEGIN WORK;

 

        LOCK TABLE {dbSchema}.configuration_registry IN ROW EXCLUSIVE MODE;

 

        WITH filter AS (

        SELECT id FROM {dbSchema}.configuration_registry

        WHERE

          in_process = FALSE

          AND

          CURRENT_TIMESTAMP  >= coalesce(last_check_time + interval '1 minute'*check_period_minutes, CURRENT_TIMESTAMP)

          AND

          NOT (id = ANY(coalesce(:excludeIds, array[]::int[])))

        ORDER BY id

        LIMIT  1

        FOR UPDATE)

 

        UPDATE {dbSchema}.configuration_registry

        SET

           in_process = true,          

           last_check_time = CURRENT_TIMESTAMP

           FROM filter

        WHERE configuration_registry.id = filter.id

        RETURNING *;          

 

        COMMIT WORK;";

       

        /// <summary>

        /// Запрос на именение и получение одной записи в таблице configuration_registry

        /// </summary>

        /// <param name="dbSchema">Схема БД</param>

        /// <param name="id">Id записи</param>

        /// <returns></returns>

        public static string UpdateOneRecordConcurrency(string dbSchema) => $@"

        BEGIN WORK;

 

        LOCK TABLE {dbSchema}.configuration_registry IN ROW EXCLUSIVE MODE;

 

        WITH filter AS (

        SELECT id FROM {dbSchema}.configuration_registry

        WHERE

          in_process = FALSE

          AND

          id = :id         

        FOR UPDATE)

 

        UPDATE {dbSchema}.configuration_registry

        SET

           in_process = true,          

           last_check_time = CURRENT_TIMESTAMP

           FROM filter

        WHERE configuration_registry.id = filter.id

        RETURNING *;          

 

        COMMIT WORK;";

    }

}
