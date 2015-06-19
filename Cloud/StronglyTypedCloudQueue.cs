using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;


namespace Wimt.Azure.Cloud
{
    public class StronglyTypedCloudQueue<QMessage> where QMessage : QueueMessage, new()
    {
        #region Fields

        private CloudStorageAccount storageAccount;
        private CloudQueue queue;

        #endregion

        #region Properties

        protected string ConnectionString { get; private set; }
        protected string QueueName { get; private set; }

        #endregion

        #region Constructor

        public StronglyTypedCloudQueue(string connectionString, string queueName)
        {
            ConnectionString = connectionString;
            QueueName = queueName;

            storageAccount = CloudStorageAccount.Parse(ConnectionString);

            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            queue = queueClient.GetQueueReference(QueueName);

            queue.CreateIfNotExists();
        }

        #endregion

        #region Methods

        public void AddMessage(QMessage message)
        {
            //CloudQueueMessage queueMessage = new CloudQueueMessage(message.ToBinary());
            var queueMessage = new CloudQueueMessage(JsonConvert.SerializeObject(message));
            queue.AddMessage(queueMessage);
        }

        public void DeleteMessage(QMessage message)
        {
            queue.DeleteMessage(message.GetCloudQueueMessageReference());
        }

        public QMessage GetMessage(TimeSpan timeout)
        {
            CloudQueueMessage queueMessage = queue.GetMessage(timeout);

            return QueueMessage.FromMessage<QMessage>(queueMessage);
        }

        public IEnumerable<QMessage> GetMessages(int count, TimeSpan timeout)
        {
            var queueMessages = queue.GetMessages(count, timeout);

            return queueMessages.Select(q => QueueMessage.FromMessage<QMessage>(q));
        }

        #endregion
    }
}
