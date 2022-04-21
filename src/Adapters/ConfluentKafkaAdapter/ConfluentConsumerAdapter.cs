using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Parallafka.KafkaConsumer;

namespace Parallafka.Adapters.ConfluentKafka
{
    public class ConfluentConsumerAdapter<TKey, TValue> : IKafkaConsumer<TKey, TValue>
    {
        private readonly IConsumer<TKey, TValue> _confluentConsumer;

        private readonly string _topic;

        private readonly Action<Action<IReadOnlyCollection<KafkaConsumer.TopicPartition>>> _addPartitionsRevokedHandler;

        private readonly Action<Action<IReadOnlyCollection<KafkaConsumer.TopicPartition>>> _addPartitionsAssignedHandler;

        /// <summary></summary>
        /// <param name="consumer"></param>
        /// <param name="topic"></param>
        /// <param name="AddPartitionsRevokedHandler">
        /// A function that registers the callback for when partitions are revoked (e.g. during a rebalance).
        /// </param>
        public ConfluentConsumerAdapter(
            IConsumer<TKey, TValue> consumer,
            string topic,
            Action<Action<IReadOnlyCollection<KafkaConsumer.TopicPartition>>> addPartitionsRevokedHandler,
            Action<Action<IReadOnlyCollection<KafkaConsumer.TopicPartition>>> addPartitionsAssignedHandler)
        {
            this._confluentConsumer = consumer;
            this._topic = topic;
            this._addPartitionsRevokedHandler = addPartitionsRevokedHandler;
            this._addPartitionsAssignedHandler = addPartitionsAssignedHandler;

        }

        public Task CommitAsync(IKafkaMessage<TKey, TValue> message)
        {
            // TODO: Does this not accept a CancellationToken? Roll our own?
            this._confluentConsumer.Commit(new[]
            {
                new TopicPartitionOffset(
                    this._topic,
                    message.Offset.Partition,
                    message.Offset.Offset)
            });
            
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            this._confluentConsumer.Dispose();
            return ValueTask.CompletedTask;
        }

        public async Task<IKafkaMessage<TKey, TValue>> PollAsync(CancellationToken cancellationToken)
        {
            await Task.Yield();
            ConsumeResult<TKey, TValue> result;

            for (;;)
            {
                result = this._confluentConsumer.Consume(cancellationToken);
                if (result.IsPartitionEOF)
                {
                    await Task.Delay(50, cancellationToken);
                }
                else
                {
                    break;
                }
            }

            IKafkaMessage<TKey, TValue> msg = KafkaMessage.Create(
                result.Message.Key,
                result.Message.Value,
                new RecordOffset(result.Partition, result.Offset));
            return msg;
        }

        public IReadOnlyCollection<KafkaConsumer.TopicPartition> Assignment
        {
            get
            {
                return this._confluentConsumer.Assignment
                    .Select(tp => new Parallafka.KafkaConsumer.TopicPartition(tp.Topic, tp.Partition))
                    .ToArray();
            }
        }

        public Task AssignAsync(IEnumerable<KafkaConsumer.TopicPartition> topicPartitions)
        {
            this._confluentConsumer.Subscribe(this._topic);
            this._confluentConsumer.Assign(topicPartitions.Select(ToConfluentModel));
            return Task.CompletedTask;
        }

        public Task UnassignAsync()
        {
            this._confluentConsumer.Unsubscribe();
            return Task.CompletedTask;
        }

        private static Confluent.Kafka.TopicPartition ToConfluentModel(
            Parallafka.KafkaConsumer.TopicPartition topicPartition)
        {
            return new(topicPartition.Topic, topicPartition.Partition);
        }

        public void AddPartitionsRevokedHandler(Action<IReadOnlyCollection<KafkaConsumer.TopicPartition>> onPartitionsRevoked)
        {
            this._addPartitionsRevokedHandler(onPartitionsRevoked);
        }

        public void AddPartitionsAssignedHandler(Action<IReadOnlyCollection<KafkaConsumer.TopicPartition>> onPartitionsAssigned)
        {
            this._addPartitionsAssignedHandler(onPartitionsAssigned);
        }
    }
}