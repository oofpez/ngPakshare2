using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Wimt.Azure.TableStorageRepository
{
    public class TableStorageRepositoryParallelOptions
    {
        #region Properties

        public int DegreeOfPartitionParallelism { get; set; }
        public int DegreeOfBatchParallelism { get; set; }
        public CancellationToken CancellationToken { get; set; }

        #endregion

        #region Constructors

        public TableStorageRepositoryParallelOptions(int degreeOfPartitionParallelism, int degreeOfBatchParallelism)
        {
            DegreeOfPartitionParallelism = degreeOfBatchParallelism;
            DegreeOfBatchParallelism = degreeOfBatchParallelism;
        }

        public TableStorageRepositoryParallelOptions(int degreeOfPartitionParallelism, int degreeOfBatchParallelism, CancellationToken cancellationToken)
            : this(degreeOfPartitionParallelism, degreeOfBatchParallelism)
        {
            CancellationToken = cancellationToken;
        }

        #endregion
    }
}
