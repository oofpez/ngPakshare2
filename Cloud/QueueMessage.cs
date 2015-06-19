using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Wimt.Azure.Cloud
{
    [Serializable]
    public abstract class QueueMessage
    {
        [NonSerialized]
        private CloudQueueMessage cloudQueueMessage;

        public CloudQueueMessage GetCloudQueueMessageReference()
        {
            return cloudQueueMessage;
        }

        internal byte[] ToBinary()
        {
            var binaryFormatter = new BinaryFormatter();
            byte[] output;

            using (var memoryStream = new MemoryStream())
            {
                memoryStream.Position = 0;
                binaryFormatter.Serialize(memoryStream, this);
                output = memoryStream.GetBuffer();
            }

            return output;
        }

        public QueueMessage()
        {
            
        }

        internal String ToJson()
        {
            return JsonConvert.SerializeObject(this);
        }

        internal static QMessage FromMessage<QMessage>(CloudQueueMessage queueMessage)
            where QMessage : QueueMessage, new()
        {
            //QMessage message = default(QMessage);
            var message = new QMessage();

            message = JsonConvert.DeserializeObject<QMessage>(queueMessage.AsString);

            message.cloudQueueMessage = queueMessage;

            return message;

        }

        /*internal static QMessage FromMessage<QMessage>(CloudQueueMessage queueMessage) where QMessage : QueueMessage, new()
        {
            byte[] buffer = queueMessage.AsBytes;
            QMessage message = default(QMessage);

            using (MemoryStream memoryStream = new MemoryStream(buffer))
            {
                memoryStream.Position = 0;
                BinaryFormatter binaryFormatter = new BinaryFormatter();
                message = (QMessage)binaryFormatter.Deserialize(memoryStream);
            }

            message.cloudQueueMessage = queueMessage;

            return message;
        }*/
    }
}
