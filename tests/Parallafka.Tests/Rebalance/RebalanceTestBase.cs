using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Parallafka.KafkaConsumer;
using Parallafka.Tests.Helpers;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests.Rebalance
{
    public abstract class RebalanceTestBase : KafkaTopicTestBase
    {
        class MyLogger : ILogger
        {
            private RebalanceTestBase _test;
            public MyLogger(RebalanceTestBase test)
            {
                this._test = test;
            }

            class Disp : IDisposable
            {
                public void Dispose()
                {
                }
            }

            public IDisposable BeginScope<TState>(TState state)
            {
                return new Disp();
            }

            public bool IsEnabled(LogLevel logLevel)
            {
                return true;
            }

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
            {
                this._test.Console.WriteLine("LOG " + logLevel.ToString());
                if (exception != null)
                {
                    this._test.Console.WriteLine(exception.Message);
                    this._test.Console.WriteLine(exception.StackTrace);
                }
                this._test.Console.WriteLine(formatter.Invoke(state, exception));
            }
        }

        public virtual async Task TestRebalanceAsync()
        {
            var publishTask = this.PublishTestMessagesAsync(1000);
            ConcurrentQueue<IKafkaMessage<string, string>> messagesConsumedOverall = new();
            ConcurrentQueue<IKafkaMessage<string, string>> consumer1MessagesConsumed = new();
            ConcurrentQueue<IKafkaMessage<string, string>> consumer1MessagesConsumedAfterRebalance = new();
            ConcurrentQueue<IKafkaMessage<string, string>> consumer2MessagesConsumed = new();
            string consumerGroupId = $"RebalanceTest-{Guid.NewGuid()}";
            ParallafkaConfig<string, string> NewParallafkaConfig()
            {
                return new()
                {
                    MaxDegreeOfParallelism = 7,
                    Logger = new MyLogger(this),
                    MaxQueuedMessages = 20
                };
            }

            Parallafka<string, string>.WriteLine = this.Console.WriteLine;

            bool hasConsumer1Rebalanced = false;
            bool isConsumer2Started = false;
            IReadOnlyCollection<TopicPartition> partitionsRevokedFromConsumer1 = null;

            await using KafkaConsumerSpy<string, string> consumer1 = await this.Topic.GetConsumerAsync(consumerGroupId);

            Parallafka<string, string> parallafka1 = new(consumer1, NewParallafkaConfig());
            CancellationTokenSource parallafka1Cancel = new();
            Task consumeTask1 = parallafka1.ConsumeAsync(async msg =>
                {
                    messagesConsumedOverall.Enqueue(msg);
                    consumer1MessagesConsumed.Enqueue(msg);
                    if (hasConsumer1Rebalanced) // TODO: thread safety
                    {
                        consumer1MessagesConsumedAfterRebalance.Enqueue(msg);
                    }
                },
                parallafka1Cancel.Token);

            // Register this after the "real" handler registered by Parallafka
            consumer1.AddPartitionsRevokedHandler(partitions =>
            {
                if (isConsumer2Started)
                {
                    Parallafka<string, string>.WriteLine("THE REBALANCE HAS STARTED");
                    hasConsumer1Rebalanced = true;
                    partitionsRevokedFromConsumer1 = partitions;
                }
            });

            IReadOnlyCollection<TopicPartition> partitionsAssignedToConsumer1 = null;

            consumer1.AddPartitionsAssignedHandler(partitions =>
            {
                Parallafka<string, string>.WriteLine(consumer1.ToString() + "ASSIGNING PARTITIONS !!! " + string.Join(", ", partitions.Select(p => p.Partition)));
                partitionsAssignedToConsumer1 = partitions;
            });

            await Wait.UntilAsync("Consumer1 has consumed some",
                async () =>
                {
                    Assert.True(consumer1MessagesConsumed.Count > 400);
                },
                TimeSpan.FromSeconds(30));

            await using KafkaConsumerSpy<string, string> consumer2 = await this.Topic.GetConsumerAsync(consumerGroupId);
            isConsumer2Started = true;
            Parallafka<string, string> parallafka2 = new(consumer2, NewParallafkaConfig());
            CancellationTokenSource parallafka2Cancel = new();
            Task consumeTask2 = parallafka2.ConsumeAsync(async msg =>
                {
                    messagesConsumedOverall.Enqueue(msg);
                    consumer2MessagesConsumed.Enqueue(msg);
                },
                parallafka2Cancel.Token);

            await Wait.UntilAsync("Rebalanced after Consumer2 joined",
                async () =>
                {
                    Assert.True(hasConsumer1Rebalanced);
                },
                TimeSpan.FromSeconds(30));

            await Wait.UntilAsync("Consumer2 has consumed some",
                async () =>
                {
                    Assert.True(consumer2MessagesConsumed.Count > 80);
                },
                TimeSpan.FromSeconds(30));

            var messagesPublished = await publishTask;
            ConsumptionVerifier verifier = new();
            verifier.AddSentMessages(messagesPublished);
            await Wait.UntilAsync("Consumed all messages",
                async () =>
                {
                    Assert.True(messagesConsumedOverall.Count >= messagesPublished.Count);
                },
                TimeSpan.FromSeconds(45));

            // foreach (var message in consumer1MessagesConsumedAfterRebalance)
            // {
            //      // this is a problem. with current setting, it revokes ALL. So it'll fail either way.
            //      // Instead, ensure the message partition is in the set of assigned partitions per consumer.
            //     foreach (var revokedPartition in partitionsRevokedFromConsumer1)
            //     {
            //         Assert.NotEqual(revokedPartition.Partition, message.Offset.Partition);
            //     }
            // }

            foreach (var message in consumer1MessagesConsumedAfterRebalance)
            {
                Assert.Contains(message.Offset.Partition, partitionsAssignedToConsumer1.Select(p => p.Partition));
            }
            
            verifier.AddConsumedMessages(messagesConsumedOverall);
            //verifier.AssertConsumedAllSentMessagesProperly();
            //verifier.AssertAllConsumedMessagesWereCommitted
            // TODO: Add a mode that ignores duplicates so long as they are in order.
        }

        public override Task DisposeAsync()
        {
            Parallafka<string, string>.WriteLine = _ => {};
            return base.DisposeAsync();
        }

        protected RebalanceTestBase(ITestOutputHelper console) : base(console)
        {
        }
    }
}