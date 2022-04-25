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
                    MaxQueuedMessages = 200 // fails when this is 800 - TODO
                };
            }

            Parallafka<string, string>.WriteLine = this.Console.WriteLine;

            bool hasConsumer1Rebalanced = false;
            bool isConsumer2Started = false;
            IReadOnlyCollection<TopicPartition> partitionsRevokedFromConsumer1 = null;

            await using KafkaConsumerSpy<string, string> consumer1 = await this.Topic.GetConsumerAsync(consumerGroupId);

            Parallafka<string, string> parallafka1 = new(consumer1, NewParallafkaConfig());
            Func<KafkaMessageWrapped<string, string>, Task> consumer1OnMessageCommittedAsync = _ => Task.CompletedTask;
            parallafka1.OnMessageCommitted = msg =>
            {
                return consumer1OnMessageCommittedAsync.Invoke(msg);
            };
            CancellationTokenSource parallafka1Cancel = new();
            TimeSpan consumer1HandleDelay = TimeSpan.FromMilliseconds(50);
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
            long lastConsumedMsgOffset = consumer1MessagesConsumed.Last().Offset.Offset;
            await Wait.UntilAsync("Consumer1 has committed some",
                async () =>
                {
                    Assert.True(consumer1.CommittedOffsets.Any(o => o.Offset >= lastConsumedMsgOffset));
                },
                timeout: TimeSpan.FromSeconds(15));

            bool commitQueueFull = false;
            parallafka1.OnCommitQueueFull = () => commitQueueFull = true;

            // Hang the next commit
            TaskCompletionSource consumer1CommitBlocker = new();
            bool committerAppearsToBeHanging = false;
            consumer1OnMessageCommittedAsync = msg =>
            {
                committerAppearsToBeHanging = true;
                return consumer1CommitBlocker.Task;
            };
            await Wait.UntilAsync("Committer appears to be hanging as expected",
                async () =>
                {
                    Assert.True(committerAppearsToBeHanging);
                },
                timeout: TimeSpan.FromSeconds(30));
            
            var committedOffsetCountSnapshot = consumer1.CommittedOffsets.Count;

            consumer1HandleDelay = TimeSpan.Zero;
            await Wait.UntilAsync("Consumer1 has consumed some more",
                async () =>
                {
                    Assert.True(consumer1MessagesConsumed.Count > 120, $"{consumer1MessagesConsumed.Count} only");
                },
                timeout: TimeSpan.FromSeconds(30));

            await Wait.UntilAsync("Commit queue is full",
                async () =>
                {
                    Assert.True(commitQueueFull);
                },
                timeout: TimeSpan.FromSeconds(30));
            
            // Verify no new commits since blocking
            Assert.Equal(committedOffsetCountSnapshot, consumer1.CommittedOffsets.Count);

            // Unblock commits
            consumer1CommitBlocker.SetResult();
            consumer1HandleDelay = TimeSpan.FromMilliseconds(250);
            Assert.True(consumer1MessagesConsumed.Count < 500, $"Consumer1 has already consumed {consumer1MessagesConsumed.Count}");

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
                    Assert.True(hasConsumer1Rebalanced, "Consumer 1 did not rebalance"); // I guess because that call is part of Poll(), and it's stuck due to backpressure?
                },
                timeout: TimeSpan.FromSeconds(30));

            await Wait.UntilAsync("Consumer2 has consumed some",
                async () =>
                {
                    Assert.True(consumer2MessagesConsumed.Count > 80, $"Consumer2 consumed {consumer2MessagesConsumed.Count}");
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
            // Understand why this fails with out of order when rebalance-restart is commented out.
            // Would this fail WITH the solution, were the poller to reset to its most recent commit?

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