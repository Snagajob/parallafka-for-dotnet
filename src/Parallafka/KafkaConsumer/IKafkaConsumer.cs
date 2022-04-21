using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Parallafka.KafkaConsumer
{
    /// <summary>
    /// A single consumer instance of a certain Kafka topic.
    /// </summary>
    /// <typeparam name="TKey">The record key type.</typeparam>
    /// <typeparam name="TValue">The record value type.</typeparam>
    public interface IKafkaConsumer<TKey, TValue> : IAsyncDisposable
    {
        /// <summary>
        /// Polls Kafka for the next record.
        /// Returns the next record, or throws an <see cref="OperationCanceledException"/> if the cancellationToken is cancelled.
        /// </summary>
        Task<IKafkaMessage<TKey, TValue>> PollAsync(CancellationToken cancellationToken);

        Task CommitAsync(IKafkaMessage<TKey, TValue> message);

        void AddPartitionsRevokedHandler(Action<IReadOnlyCollection<TopicPartition>> onPartitionsRevoked);

        void AddPartitionsAssignedHandler(Action<IReadOnlyCollection<TopicPartition>> onPartitionsAssigned);

        IReadOnlyCollection<TopicPartition> Assignment { get; } // TODO: remove all the assignment stuff?

        Task AssignAsync(IEnumerable<TopicPartition> topicPartitions);

        Task UnassignAsync();
    }
}