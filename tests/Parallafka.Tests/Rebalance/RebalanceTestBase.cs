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
            var publishTask = this.PublishTestMessagesAsync(1000, duplicateKeys: true);
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

            TimeSpan consumer1HandleDelay = TimeSpan.FromMilliseconds(50);

            Parallafka<string, string> parallafka1 = new(consumer1, NewParallafkaConfig());
            parallafka1.OnMessageCommitted = async msg =>
            {
            };
            CancellationTokenSource parallafka1Cancel = new();
            Task consumeTask1 = parallafka1.ConsumeAsync(async msg =>
                {
                    messagesConsumedOverall.Enqueue(msg);
                    consumer1MessagesConsumed.Enqueue(msg);
                    if (hasConsumer1Rebalanced) // TODO: thread safety
                    {
                        consumer1MessagesConsumedAfterRebalance.Enqueue(msg);
                    }
                    await Task.Delay(consumer1HandleDelay);
                },
                parallafka1Cancel.Token);

            // Register this after the "real" handler registered by Parallafka.ConsumeAsync
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
                    Assert.True(consumer1MessagesConsumed.Count > 40);
                },
                timeout: TimeSpan.FromSeconds(30));

            await using KafkaConsumerSpy<string, string> consumer2 = await this.Topic.GetConsumerAsync(consumerGroupId);
            isConsumer2Started = true;
            Parallafka<string, string> parallafka2 = new(consumer2, NewParallafkaConfig());
            parallafka2.OnMessageCommitted = parallafka1.OnMessageCommitted;
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
                timeout: TimeSpan.FromSeconds(30));

            await Wait.UntilAsync("Consumer2 has consumed some",
                async () =>
                {
                    Assert.True(consumer2MessagesConsumed.Count > 80);
                },
                timeout: TimeSpan.FromSeconds(30));

            var messagesPublished = await publishTask;
            ConsumptionVerifier verifier = new();
            verifier.AddSentMessages(messagesPublished);

            consumer1HandleDelay = TimeSpan.Zero;
            await Wait.UntilAsync("Consumed all messages",
                async () =>
                {
                    Assert.True(messagesConsumedOverall.Count >= messagesPublished.Count);
                },
                timeout: TimeSpan.FromSeconds(45));

            foreach (var message in consumer1MessagesConsumedAfterRebalance)
            {
                Assert.Contains(message.Offset.Partition, partitionsAssignedToConsumer1.Select(p => p.Partition));
            }

            verifier.AddConsumedMessages(messagesConsumedOverall);
            verifier.AssertConsumedAllSentMessagesProperly();

            await Wait.UntilAsync("All messages were committed",
                async () =>
                {
                    verifier.AssertAllConsumedMessagesWereCommitted(
                        consumer1.CommittedOffsets.Concat(consumer2.CommittedOffsets));
                },
                timeout: TimeSpan.FromSeconds(30));
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