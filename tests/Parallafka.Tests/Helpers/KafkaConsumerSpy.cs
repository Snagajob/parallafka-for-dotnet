using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;

namespace Parallafka.Tests
{
    public class KafkaConsumerSpy<TKey, TValue> : IKafkaConsumer<TKey, TValue>
    {
        public ConcurrentQueue<IRecordOffset> CommittedOffsets { get; } = new();

        public IReadOnlyCollection<TopicPartition> Assignment => this._backingConsumer.Assignment;

        private readonly IKafkaConsumer<TKey, TValue> _backingConsumer;

        public KafkaConsumerSpy(IKafkaConsumer<TKey, TValue> backingConsumer)
        {
            this._backingConsumer = backingConsumer;
        }

        public Task CommitAsync(IKafkaMessage<TKey, TValue> message, CancellationToken cancelToken)
        {
            this.CommittedOffsets.Enqueue(message.Offset);
            return this._backingConsumer.CommitAsync(message, cancelToken);
        }

        public ValueTask DisposeAsync()
        {
            return this._backingConsumer.DisposeAsync();
        }

        public Task<IKafkaMessage<TKey, TValue>> PollAsync(CancellationToken cancellationToken)
        {
            return this._backingConsumer.PollAsync(cancellationToken);
        }

        public Task AssignAsync(IEnumerable<TopicPartition> topicPartitions)
        {
            throw new NotImplementedException(); //return this._backingConsumer.AssignAsync(topicPartitions);
        }

        public Task UnassignAsync()
        {
            throw new NotImplementedException(); //return this._backingConsumer.UnassignAsync();
        }

        public void AddPartitionsRevokedHandler(Action<IReadOnlyCollection<TopicPartition>> onPartitionsRevoked)
        {
            this._backingConsumer.AddPartitionsRevokedHandler(onPartitionsRevoked);
        }

        public void AddPartitionsAssignedHandler(Action<IReadOnlyCollection<TopicPartition>> onPartitionsAssigned)
        {
            this._backingConsumer.AddPartitionsAssignedHandler(onPartitionsAssigned);
        }
    }
}