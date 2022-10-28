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

        /*

        - When a rebalance is initiated, Parallafka consumers/instances that are to lose partitions do not handle or commit messages from lost partitions
        after the rebalance.

        - After a rebalance, if there are partitions NOT revoked from an instance, the messages are all still processed and committed.
        (Maybe multiple times, but in order and eventually consistent.)

        - When we detect that partitions are lost or revoked despite not receiving a pre-rebalance callback, we make
        a best-effort attempt to restore normal consumption without losing messages overall or risking race conditions from
        multiple consumers handling the same partition.
        This plays nicely with the rebalance callbacks if both occur.

        - Supports back-to-back rebalances occurring during a running rebalance callback.

        - Test the tests. Show that they fail without the rebalance logic.



        Start C1. Consume a bit. Produce a bunch of messages with the same key for each partition and
        ensure they are queued up before handling. Pause handling and allow for polling to a point, to accumulate these.
        This would mean, without rebalance logic, that revoked-partition messages would be handled post-rebalance when handling resumes.
        Start C2. Resume C1 handler. Assert rebalance. Finish consuming all messages.
        Assert that C1 did not consume or commit any messages from its revoked partitions after the rebalance.
        Assert that the overall handling order is correct, allowing for replays.
        Assert that all messages were handled, showing that we didn't purge queues of retained partitions.


        Start C1. Consume a bit. Produce a bunch of messages with the same key for each partition and
        ensure they are queued up before handling. Pause handling and allow for polling to a point, to accumulate these.
        This would mean, without rebalance logic, that revoked-partition messages would be handled post-rebalance when handling resumes.
        Somehow we need to cause or simulate the errors we get on commit calls when the consumer no longer
        owns the partition. Maybe we override the normal rebalance callback logic to simulate receiving the errors without
        receiving the callbacks. The point is to show that the errors trigger the same resolution as the graceful-rebalance callbacks.


        Start C1. Consume a bit. Produce a bunch of messages with the same key for each partition and
        ensure they are queued up before handling. Pause handling and allow for polling to a point, to accumulate these.
        This would mean, without rebalance logic, that revoked-partition messages would be handled post-rebalance when handling resumes.
        Somehow we need to cause or simulate the errors we get on commit calls when the consumer no longer
        owns the partition. Maybe we override the normal rebalance callback logic to simulate receiving the errors without
        receiving the callbacks. The point is to show that the errors trigger the same resolution as the graceful-rebalance callbacks.
        We also want to demonstrate that the errors can be received and handled in tandem with the callbacks.
        Trigger the errors, then the callbacks, and then the callbacks before the erorrs,
        and assert that the completion & order outcome is correct.


        For all of these tests, have a version where the rebalance logic is disabled, and assert that their assertions fail.

        Test cooperative rebalancing and default.

        */

        public virtual async Task TestRebalanceAsync()
        {
            var publishTask = this.PublishTestMessagesAsync(2500, duplicateKeys: true);
            ConcurrentQueue<IKafkaMessage<string, string>> messagesConsumedOverall = new();
            ConcurrentQueue<IKafkaMessage<string, string>> consumer1MessagesConsumed = new();
            ConcurrentQueue<IKafkaMessage<string, string>> consumer1MessagesConsumedAfterRebalance = new();
            ConcurrentQueue<IKafkaMessage<string, string>> consumer2MessagesConsumed = new();
            HashSet<int> partitionsAssignedToConsumer1 = new();
            string consumerGroupId = $"RebalanceTest-{Guid.NewGuid()}";
            ParallafkaConfig<string, string> NewParallafkaConfig()
            {
                return new()
                {
                    MaxDegreeOfParallelism = 13,
                    Logger = new MyLogger(this),
                    MaxQueuedMessages = 3000
                };
            }

            Parallafka<string, string>.WriteLine = this.Console.WriteLine;

            HashSet<int> consumer1PartitionsBeingHandled = new();
            bool monitorConsumer1PartitionsBeingHandled = false;

            bool hasConsumer1Rebalanced = false;
            bool isConsumer2Started = false;

            await using KafkaConsumerSpy<string, string> consumer1 = await this.Topic.GetConsumerAsync(consumerGroupId);

            Parallafka<string, string> parallafka1 = new(consumer1, NewParallafkaConfig());
            Func<KafkaMessageWrapped<string, string>, Task> consumer1OnMessageCommittedAsync = _ => Task.CompletedTask;
            parallafka1.OnMessageCommitted = msg =>
            {
                return consumer1OnMessageCommittedAsync.Invoke(msg);
            };
            CancellationTokenSource parallafka1Cancel = new();
            TimeSpan consumer1HandleDelay = TimeSpan.FromMilliseconds(50);
            Func<Task> onConsumer1ConsumedAsync = () => Task.CompletedTask;

            TaskCompletionSource partitionHandlerThreadPause1 = new();
            TaskCompletionSource partitionHandlerThreadPause2 = new();
            Func<int, Task> onConsumer1HandlerThreadWonPartitionAsync = _ => Task.CompletedTask;

            SemaphoreSlim rebalanceLock = new(1);

            Task consumeTask1 = parallafka1.ConsumeAsync(async msg =>
                {
                    bool wonPartition = false;
                    if (monitorConsumer1PartitionsBeingHandled)
                    {
                        lock (consumer1PartitionsBeingHandled)
                        {
                            wonPartition = consumer1PartitionsBeingHandled.Add(msg.Offset.Partition);
                        }
                        if (wonPartition)
                        {
                            await onConsumer1HandlerThreadWonPartitionAsync(msg.Offset.Partition);
                        }
                    }
                    
                    messagesConsumedOverall.Enqueue(msg);
                    consumer1MessagesConsumed.Enqueue(msg);

                    await rebalanceLock.WaitAsync();
                    if (hasConsumer1Rebalanced) // TODO: thread safety
                    {
                        consumer1MessagesConsumedAfterRebalance.Enqueue(msg);
                        if (!partitionsAssignedToConsumer1.Contains(msg.Offset.Partition))
                        {
                            throw new Exception($"BOOM {msg.Offset.Partition} not assigned to c1"); // todo remove
                        }
                    }
                    rebalanceLock.Release();

                    await onConsumer1ConsumedAsync();
                    await Task.Delay(consumer1HandleDelay);

                    if (monitorConsumer1PartitionsBeingHandled && wonPartition)
                    {
                        lock (consumer1PartitionsBeingHandled)
                        {
                            consumer1PartitionsBeingHandled.Remove(msg.Offset.Partition);
                        }
                    }
                },
                parallafka1Cancel.Token);

            // Register this after the "real" handler registered by Parallafka.ConsumeAsync
            consumer1.AddPartitionsRevokedHandler(partitions =>
            {
                rebalanceLock.Wait();
                int[] partitionsRevoked = partitions.Select(ptn => ptn.Partition).ToArray();
                Parallafka<string, string>.WriteLine("C1 before revoked: " + string.Join(",", partitionsAssignedToConsumer1));
                partitionsAssignedToConsumer1.RemoveWhere(partitionsRevoked.Contains);
                Parallafka<string, string>.WriteLine("C1 after revoked: " + string.Join(",", partitionsAssignedToConsumer1));

                if (isConsumer2Started)
                {
                    Parallafka<string, string>.WriteLine("THE REBALANCE HAS STARTED");
                    hasConsumer1Rebalanced = true;
                }
                rebalanceLock.Release();
            });

            consumer1.AddPartitionsAssignedHandler(partitions =>
            {
                rebalanceLock.Wait();
                Parallafka<string, string>.WriteLine(consumer1.ToString() + "C1 ASSIGNING PARTITIONS !!! " + string.Join(", ", partitions.Select(p => p.Partition)));
                foreach (var partition in partitions.Select(p => p.Partition))
                {
                    partitionsAssignedToConsumer1.Add(partition);
                }
                rebalanceLock.Release();
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

            // await Wait.UntilAsync("Same-key messages are queued to be handled for each partition",
            //     async () =>
            //     {

            //     },
            //     timeout: TimeSpan.FromSeconds(30));

            monitorConsumer1PartitionsBeingHandled = true;
            onConsumer1HandlerThreadWonPartitionAsync = partition => partition % 2 == 0 ?
                partitionHandlerThreadPause1.Task :
                partitionHandlerThreadPause2.Task;
            await Wait.UntilAsync("One message for each partition is being handled by a paused handler",
                async () =>
                {
                    Assert.True(messagesConsumedOverall.Count < 2500);
                    Assert.Equal(11, consumer1PartitionsBeingHandled.Count);
                },
                timeout: TimeSpan.FromSeconds(45));

            partitionHandlerThreadPause1.SetResult();
            partitionHandlerThreadPause2.SetResult();
            monitorConsumer1PartitionsBeingHandled = false;

            // Hang the next commit
            TaskCompletionSource consumer1CommitBlocker = new();
            bool committerAppearsToBeHanging = false;
            int consumer1MessagesConsumedCountBeforeCommitterHang = -1;
            consumer1OnMessageCommittedAsync = msg =>
            {
                committerAppearsToBeHanging = true;
                consumer1MessagesConsumedCountBeforeCommitterHang = consumer1MessagesConsumed.Count;
                return consumer1CommitBlocker.Task;
            };
            await Wait.UntilAsync("Committer appears to be hanging as expected",
                async () =>
                {
                    Assert.True(committerAppearsToBeHanging);
                },
                timeout: TimeSpan.FromSeconds(30));
            await Wait.UntilAsync("Consumer1 handled some messages with committer stuck",
                async () =>
                {
                    Assert.True(consumer1MessagesConsumed.Count - consumer1MessagesConsumedCountBeforeCommitterHang > 50);
                },
                timeout: TimeSpan.FromSeconds(30));

            // Hang consumer1's handler while we let consumer2 come online
            TaskCompletionSource consumer1HandlerHang = new();
            onConsumer1ConsumedAsync = () => consumer1HandlerHang.Task;
            //Assert.True(consumer1MessagesConsumed.Count < 500, $"Consumer1 has already consumed {consumer1MessagesConsumed.Count}");

            await using KafkaConsumerSpy<string, string> consumer2 = await this.Topic.GetConsumerAsync(consumerGroupId);
            isConsumer2Started = true;
            Parallafka<string, string> parallafka2 = new(consumer2, NewParallafkaConfig());
            CancellationTokenSource parallafka2Cancel = new();
            Task consumeTask2 = parallafka2.ConsumeAsync(async msg =>
                {
                    messagesConsumedOverall.Enqueue(msg);
                    consumer2MessagesConsumed.Enqueue(msg);
                    //await Task.Delay(500);
                },
                parallafka2Cancel.Token);
            
            // Resume consumer1 handler to allow rebalance
            consumer1HandlerHang.SetResult();

            // Unblock every other handler thread. After rebalance, assert that some of the blocked-handler partitions running in C1
            // have been transferred to C2.

            await Wait.UntilAsync("Rebalanced after Consumer2 joined",
                async () =>
                {
                    this.Console.WriteLine($"{consumer1MessagesConsumed.Count.ToString()} consumed by consumer1");
                    //Assert.True(consumer1MessagesConsumed.Count < 1500, $"Consumer1 has already consumed {consumer1MessagesConsumed.Count}");
                    Assert.True(hasConsumer1Rebalanced, "Consumer 1 did not rebalance"); // I guess because that call is part of Poll(), and it's stuck due to backpressure?
                    //Assert.False(commitQueueFull);
                },
                timeout: TimeSpan.FromSeconds(333));

            // partitionHandlerThreadPause.SetResult();
            // monitorConsumer1PartitionsBeingHandled = false;

            // Unblock commits
            consumer1CommitBlocker.SetResult();

            await Wait.UntilAsync("Consumer2 has consumed some",
                async () =>
                {
                    Assert.True(consumer2MessagesConsumed.Count > 80, $"Consumer2 consumed {consumer2MessagesConsumed.Count}");
                },
                timeout: TimeSpan.FromSeconds(30));

            IReadOnlyCollection<IKafkaMessage<string, string>> messagesPublished = await publishTask;
            ConsumptionVerifier verifier = new();
            verifier.AddSentMessages(messagesPublished);

            consumer1HandleDelay = TimeSpan.Zero;
            
            await Wait.UntilAsync("Consumed all messages",
                async () =>
                {
                    HashSet<string> messageIdsConsumedOverall = new(messagesConsumedOverall.Select(ConsumptionVerifier.UniqueIdFor));
                    foreach (var msg in messagesPublished)
                    {
                        string id = ConsumptionVerifier.UniqueIdFor(msg);
                        Assert.True(messageIdsConsumedOverall.Contains(id));
                    }
                    // This is wrong. Dupes. It was still consuming...
                    //Assert.True(messagesConsumedOverall.Count >= messagesPublished.Count);
                },
                timeout: TimeSpan.FromSeconds(45));

            // TODO: Enhance this to map messages consumed in each "balance" era, storing valid assigned partitions with all the messages handled.
            this.Console.WriteLine("there are " + consumer1MessagesConsumedAfterRebalance.Count);
            //Assert.Equal(0, consumer1MessagesConsumedAfterRebalance.Count(m => !partitionsAssignedToConsumer1.Contains(m.Offset.Partition)));
            foreach (var message in consumer1MessagesConsumedAfterRebalance) 
            {
                Assert.Contains(message.Offset.Partition, partitionsAssignedToConsumer1);
            }

            // why is this passing when we pause commits? Because when we unblock commits, commit happens for all the handled messages before the rebalance?
            // Should we pause some handler threads?
            // When we move the commitUnblock() to after the rebalance, well, rebalance never happens with commits blocked.

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