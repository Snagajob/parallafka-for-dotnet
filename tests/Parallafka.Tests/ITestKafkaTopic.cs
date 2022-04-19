using System.Collections.Generic;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;

namespace Parallafka.Tests
{
    public interface ITestKafkaTopic // TODO: Make Disposable
    {
        string Name { get; }

        Task InitializeAsync();
        
        Task DeleteAsync();

        /// <summary>
        /// Returns a consumer instance for the given consumer group ID.
        /// </summary>
        Task<KafkaConsumerSpy<string, string>> GetConsumerAsync(string groupId);
        
        /// <summary>
        /// Publishes the given messages to the test topic in order.
        /// </summary>
        Task PublishAsync(IEnumerable<IKafkaMessage<string, string>> messages);
    }
}