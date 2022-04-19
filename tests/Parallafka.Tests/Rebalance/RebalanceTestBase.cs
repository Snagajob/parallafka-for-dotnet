using System;
using System.Collections.Concurrent;
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
                    Logger = new MyLogger(this)
                };
            }

            Parallafka<string, string>.WriteLine = this.Console.WriteLine;

            bool hasConsumer1Rebalanced = false;
            bool isConsumer2Started = false;

            await using KafkaConsumerSpy<string, string> consumer1 = await this.Topic.GetConsumerAsync(consumerGroupId);

            consumer1.AddPartitionsRevokedHandler(partitions =>
            {
                if (isConsumer2Started)
                {
                    Parallafka<string, string>.WriteLine("THE REBALANCE HAS OCCURRED");
                    hasConsumer1Rebalanced = true;
                }
            });

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

            await Wait.UntilAsync("Consumer1 has consumed some",
                async () =>
                {
                    Assert.True(consumer1MessagesConsumed.Count > 80);
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

            await Wait.UntilAsync("Consumer2 has consumed some",
                async () =>
                {
                    Assert.True(consumer2MessagesConsumed.Count > 80);
                },
                TimeSpan.FromSeconds(30));

            await Wait.UntilAsync("Rebalanced for consumer2 join",
                async () =>
                {
                    Assert.True(hasConsumer1Rebalanced);
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
            
            verifier.AddConsumedMessages(messagesConsumedOverall);
            //verifier.AssertConsumedAllSentMessagesProperly();
            //verifier.AssertAllConsumedMessagesWereCommitted
        }

        protected RebalanceTestBase(ITestOutputHelper console) : base(console)
        {
        }
    }
}