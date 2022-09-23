using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Parallafka.KafkaConsumer;
using Xunit;

namespace Parallafka.Tests.Helpers
{
    public class ConsumptionVerifier
    {
        private readonly List<IKafkaMessage<string, string>> _sentMessages = new();

        private readonly HashSet<string> _sentMessageUniqueIds = new();

        private readonly HashSet<string> _consumedMessageUniqueIds = new();

        private readonly Dictionary<string, Queue<IKafkaMessage<string, string>>> _consumedMessagesByKey = new();

        private int _consumedMessageCount = 0;

        public static string UniqueIdFor(IKafkaMessage<string, string> message)
        {
            // TODO: Override equals
            return $"{message.Key}-[ :) ]-{message.Value}";
        }

        public void AddSentMessages(IEnumerable<IKafkaMessage<string, string>> messages)
        {
            lock (this._sentMessages)
            {
                foreach (var message in messages)
                {
                    string id = UniqueIdFor(message);
                    if (!this._sentMessageUniqueIds.Add(id))
                    {
                        throw new Exception($"Sent message already added: {id}");
                    }

                    this._sentMessages.Add(message);
                }
            }
        }

        /// <summary>
        /// Call this when messages are received by a consumer. Thread-safe.
        /// </summary>
        public void AddConsumedMessages(IEnumerable<IKafkaMessage<string, string>> messages)
        {
            foreach (var msg in messages)
            {
                Interlocked.Increment(ref this._consumedMessageCount);

                lock (this._consumedMessagesByKey)
                {
                    if (!this._consumedMessagesByKey.TryGetValue(msg.Key, out var queueForKey))
                    {
                        queueForKey = new();
                        this._consumedMessagesByKey[msg.Key] = queueForKey;
                    }

                    queueForKey.Enqueue(msg);
                    this._consumedMessageUniqueIds.Add(UniqueIdFor(msg));
                }
            }
        }

        public void AddConsumedMessage(IKafkaMessage<string, string> message)
        {
            this.AddConsumedMessages(new[] { message });
        }

        public void AssertConsumedAllSentMessagesProperly()
        {
            Assert.Equal(this._sentMessages.Count, this._consumedMessageUniqueIds.Count);

            foreach (var sentMessage in this._sentMessages)
            {
                Assert.True(this._consumedMessageUniqueIds.Contains(UniqueIdFor(sentMessage)));
            }

            foreach (var kvp in this._consumedMessagesByKey)
            {
                long prevMsgOffset = -1;
                List<IKafkaMessage<string, string>> messagesInConsumeOrderForThisKey = new(kvp.Value);
                for (int i = 0; i < messagesInConsumeOrderForThisKey.Count; i++)
                {
                    IKafkaMessage<string, string> message = messagesInConsumeOrderForThisKey[i];

                    Assert.Equal(kvp.Key, message.Key);
                    
                    long offset = message.Offset.Offset;

                    if (offset < prevMsgOffset)
                    {
                        // This is normally a bad sign, but let's see if this is the valid edge case.
                        // It's okay if we handle offset 94 after 95 so long as we handle 95 again.
                        // This can happen especially during consumer rebalances when handled messages are not committed before shutdown.
                        bool nextMessageOffsetIsPrevMsgOffset = i + 1 < messagesInConsumeOrderForThisKey.Count &&
                            messagesInConsumeOrderForThisKey[i + 1].Offset.Offset == prevMsgOffset;
                        bool isValidOutOfOrderEdgeCase = offset == prevMsgOffset - 1 && nextMessageOffsetIsPrevMsgOffset;
                        if (!isValidOutOfOrderEdgeCase)
                        {
                            Assert.True(offset >= prevMsgOffset, $"{offset} not >= previous message offset {prevMsgOffset}");
                        }
                    }

                    prevMsgOffset = offset;
                }
            }
        }

        public void AssertAllConsumedMessagesWereCommitted(KafkaConsumerSpy<string, string> consumer)
        {
            this.AssertAllConsumedMessagesWereCommitted(consumer.CommittedOffsets);
        }

        public void AssertAllConsumedMessagesWereCommitted(IEnumerable<IRecordOffset> offsetsCommitted)
        {
            var byPartition = this._consumedMessagesByKey.Values
                .SelectMany(q => q)
                .GroupBy(m => m.Offset.Partition);

            foreach (var partition in byPartition)
            {
                var maxOffset = partition.Max(g => g.Offset.Offset);

                if (!offsetsCommitted.Any(offset =>
                    offset.Partition == partition.Key && offset.Offset >= maxOffset))
                {
                    Assert.False(true, $"Expecting to find committed offset for P:{partition.Key} O:{maxOffset}");
                }
            }
        }
    }
}