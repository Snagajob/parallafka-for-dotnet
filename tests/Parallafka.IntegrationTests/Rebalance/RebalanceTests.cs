using System;
using System.Threading.Tasks;
using Parallafka.Tests;
using Parallafka.Tests.Rebalance;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.IntegrationTests.Rebalance
{
    public class RebalanceTests : RebalanceTestBase
    {
        [Fact]
        public override Task TestRebalanceAsync()
        {
            return base.TestRebalanceAsync();
        }

        private readonly RealKafkaTopicProvider _topic = new($"ParallafkaRebalanceTest-{Guid.NewGuid()}");
        protected override ITestKafkaTopic Topic => this._topic;
        public RebalanceTests(ITestOutputHelper console) : base(console)
        {
        }
    }
}