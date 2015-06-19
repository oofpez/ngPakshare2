using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Wimt.Azure.TableStorageRepository
{
    public class TableStorageRepository<TEntity> where TEntity : ITableEntity, new()
    {
        #region Fields

        protected CloudStorageAccount storageAccount;

        protected CloudTable table;

        #endregion

        #region Properties

        protected string ConnectionString { get; private set; }

        protected string TableName { get; private set; }

        #endregion

        #region Constructor

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="tableName"></param>
        public TableStorageRepository(string connectionString, string tableName)
            : this(connectionString, tableName, true)
        { }

        /// <summary>
        /// Constructor with Nagle's algorithm
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="tableName"></param>
        /// <param name="useNaglesAlgorithm"></param>
        public TableStorageRepository(string connectionString, string tableName, bool useNaglesAlgorithm)
        {
            ConnectionString = connectionString;
            TableName = tableName;

            storageAccount = CloudStorageAccount.Parse(ConnectionString);

            ServicePoint tableServicePoint = ServicePointManager.FindServicePoint(storageAccount.TableEndpoint);
            tableServicePoint.UseNagleAlgorithm = useNaglesAlgorithm;

            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            tableClient.ServerTimeout = TimeSpan.FromSeconds(2);

            //tableClient.DefaultRequestOptions.ServerTimeout = TimeSpan.FromSeconds(2);

            table = tableClient.GetTableReference(TableName);

            table.CreateIfNotExists();
        }

        #endregion

        #region Methods

        /// <summary>
        /// Inserts single entity.
        /// </summary>
        /// <param name="entity">TEntity object</param>
        public void InsertOrReplace(TEntity entity)
        {
            TableOperation insertOperation = TableOperation.InsertOrReplace(entity);
            table.Execute(insertOperation);
        }

        public void InsertOrReplaceBatch(List<TEntity> entities)
        {
            if (entities.Count() > 100)
            {
                throw new ArgumentOutOfRangeException("Batch inserting cannot exceed 100 entities");
            }

            if (entities.Count() > entities.Select(x => x.RowKey).Distinct().Count())
            {
                throw new InvalidOperationException("Duplicate RowKeys not allowed in batch inserts");
            }

            // Insert batches in groups by partition key
            foreach (var partitionKey in entities.Select(x => x.PartitionKey).Distinct())
            {
                TableBatchOperation batchOperation = new TableBatchOperation();

                foreach (var entity in entities.Where(x => x.PartitionKey.Equals(partitionKey)))
                {
                    batchOperation.InsertOrReplace(entity);
                }

                table.ExecuteBatch(batchOperation);
            }
        }

        /// <summary>
        /// Batch inserts an IEnumerable of entities in parallel.
        /// </summary>
        /// <param name="entities">IEnumerable of TEntity entities</param>
        /// <param name="insertParallelOptions">Options for parallelism</param>
        public void InsertOrReplaceParallel(IEnumerable<TEntity> entities, TableStorageRepositoryParallelOptions insertParallelOptions)
        {
            ParallelOptions parallelOptionsPartition = new ParallelOptions();
            parallelOptionsPartition.MaxDegreeOfParallelism = insertParallelOptions.DegreeOfPartitionParallelism;
            parallelOptionsPartition.CancellationToken = insertParallelOptions.CancellationToken;

            ParallelOptions parallelOptionsBatch = new ParallelOptions();
            parallelOptionsBatch.MaxDegreeOfParallelism = insertParallelOptions.DegreeOfBatchParallelism;
            parallelOptionsBatch.CancellationToken = insertParallelOptions.CancellationToken;

            Parallel.ForEach(entities.GroupBy(x => x.PartitionKey), parallelOptionsPartition, entitiesByPartition =>
            {
                Parallel.ForEach(entitiesByPartition.Select((x, i) => new { Index = i, Value = x }).GroupBy(x => x.Index / 100).Select(x => x.Select(v => v.Value)), parallelOptionsBatch, entitiesByLimit =>
                {
                    TableBatchOperation batchOperation = new TableBatchOperation();

                    foreach (var entity in entitiesByLimit)
                    {
                        batchOperation.InsertOrReplace(entity);
                    }

                    var results = table.ExecuteBatch(batchOperation);
                });
            });
        }

        /// <summary>
        /// Batch inserts an IEnumerable of entities sequentially.
        /// </summary>
        /// <param name="entities">IEnumerable of TEntity entities</param>
        public void InsertOrReplace(IEnumerable<TEntity> entities)
        {
            TableStorageRepositoryParallelOptions parallelOptions = new TableStorageRepositoryParallelOptions(1, 1);

            InsertOrReplaceParallel(entities, parallelOptions);
        }

        /// <summary>
        /// Retrieves entity using the partition key and row key.
        /// </summary>
        /// <param name="partitionKey">Partition of entity</param>
        /// <param name="rowKey">Row key of entity</param>
        /// <returns>TEntity entity from Table Storage</returns>
        public TEntity Find(string partitionKey, string rowKey)
        {
            TableOperation retrieveOperation = TableOperation.Retrieve<TEntity>(partitionKey, rowKey);
            TableResult retrievedResult = table.Execute(retrieveOperation);

            TEntity entity = (TEntity)retrievedResult.Result;

            return entity;
        }

        /// <summary>
        /// Retrieves all entities belonging to a partition.
        /// </summary>
        /// <param name="partitionKey">Partition</param>
        /// <returns>IEnumerable of TEntity entities</returns>
        public IEnumerable<TEntity> Find(string partitionKey)
        {
            var queryString = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);
            var query = (new TableQuery<TEntity>()).Where(queryString);

            return table.ExecuteQuery(query);
        }

        /// <summary>
        /// Executes a TableQuery on a table and returns the result.
        /// </summary>
        /// <param name="query">TableQuery</param>
        /// <returns>IEnumerable of TEntity entities</returns>
        public IEnumerable<TEntity> Query(TableQuery<TEntity> query)
        {
            return table.ExecuteQuery(query);
        }

        /// <summary>
        /// Deletes an entity from the storage table.
        /// </summary>
        /// <param name="entity">TEntity object</param>
        public void Delete(TEntity entity)
        {
            TableOperation deleteOperation = TableOperation.Delete(entity);
            table.Execute(deleteOperation);
        }

        public void DeleteBatch(IEnumerable<TEntity> entities)
        {
            foreach (var entity in entities)
            {
                Delete(entity);
            }
        }

        #endregion
    } 
}
