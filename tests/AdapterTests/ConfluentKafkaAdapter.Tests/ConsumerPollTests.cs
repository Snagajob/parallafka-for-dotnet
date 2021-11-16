using System;
using System.Threading.Tasks;
using Parallafka.IntegrationTests;
using Parallafka.Tests;
using Parallafka.Tests.Contracts;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.AdapterTests.ConfluentKafkaAdapter.Tests
{
    public class ConsumerPollTests : ConsumerPollTestsBase
    {
        private TestKafkaTopicProvider _topic = new TestKafkaTopicProvider($"ParallafkaConsumerPollTests-{Guid.NewGuid().ToString()}");
        
        protected override ITestKafkaTopic Topic => this._topic;

        [Fact]
        public override Task ConsumerHangsAtPartitionEndsTillNewMessageAsync()
        {
            return base.ConsumerHangsAtPartitionEndsTillNewMessageAsync();
        }

        [Fact]
        public override Task RawConsumerHangsAtPartitionEndsTillNewMessageOrCancellationAsync()
        {
            return base.RawConsumerHangsAtPartitionEndsTillNewMessageOrCancellationAsync();
        }

        public ConsumerPollTests(ITestOutputHelper console) : base(console)
        {
        }
    }
}