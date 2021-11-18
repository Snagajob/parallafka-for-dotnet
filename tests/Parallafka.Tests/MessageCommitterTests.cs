﻿using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Moq;
using Parallafka.KafkaConsumer;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests
{
    public class MessageCommitterTests
    {
        private readonly ITestOutputHelper _output;

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CommitsSimpleRecordAsync(bool wasHandled)
        {
            Parallafka<string, string>.WriteLine = s => this._output.WriteLine(s);
            // given
            var consumer = new Mock<IKafkaConsumer<string, string>>();
            var logger = new Mock<ILogger>();
            var commitState = new CommitState<string, string>(int.MaxValue, default);
            var kafkaMessage = new KafkaMessage<string, string>("key", "value", new RecordOffset(0, 0));
            await commitState.EnqueueMessageAsync(kafkaMessage);
            var mc = new MessageCommitter<string, string>(
                consumer.Object,
                commitState,
                logger.Object,
                TimeSpan.FromDays(1),
                default);
            kafkaMessage.WasHandled = wasHandled;

            // when
            await mc.CommitNow();
            mc.Complete();
            await mc.Completion;

            this._output.WriteLine("Wait mc.Completion finished");

            // then
            consumer.Verify(c => c.CommitAsync(It.Is<IRecordOffset>(r => r.Offset == 0 && r.Partition == 0)),
                wasHandled
                    ? Times.Once
                    : Times.Never);
            consumer.VerifyNoOtherCalls();

            Assert.Empty(commitState.GetMessagesToCommit());
        }

        [Fact]
        public async Task CommitsOutOfOrderCorrectlyAsync()
        {
            // given
            var consumer = new Mock<IKafkaConsumer<string, string>>();
            var logger = new Mock<ILogger>();
            var commitState = new CommitState<string, string>(int.MaxValue, default);
            var kafkaMessage1 = new KafkaMessage<string, string>("key", "value", new RecordOffset(0, 1));
            var kafkaMessage2 = new KafkaMessage<string, string>("key", "value", new RecordOffset(0, 2));
            var kafkaMessage3 = new KafkaMessage<string, string>("key", "value", new RecordOffset(0, 3));
            await commitState.EnqueueMessageAsync(kafkaMessage1);
            await commitState.EnqueueMessageAsync(kafkaMessage2);
            await commitState.EnqueueMessageAsync(kafkaMessage3);
            var mc = new MessageCommitter<string, string>(
                consumer.Object,
                commitState,
                logger.Object,
                TimeSpan.FromDays(1),
                default);
            kafkaMessage1.WasHandled = true;
            kafkaMessage2.WasHandled = true;
            kafkaMessage3.WasHandled = false;

            // when
            await mc.CommitNow();
            mc.Complete();
            await mc.Completion;

            // then
            consumer.Verify(c => c.CommitAsync(It.Is<IRecordOffset>(r => r.Equals(kafkaMessage2.Offset))), Times.Once);
            consumer.VerifyNoOtherCalls();

            Assert.Empty(commitState.GetMessagesToCommit());
        }
        
        [Fact]
        public async Task CommitsLatestCorrectlyAsync()
        {
            // given
            var consumer = new Mock<IKafkaConsumer<string, string>>();
            var logger = new Mock<ILogger>();
            var commitState = new CommitState<string, string>(int.MaxValue, default);
            var kafkaMessage1 = new KafkaMessage<string, string>("key", "value", new RecordOffset(0, 1));
            var kafkaMessage2 = new KafkaMessage<string, string>("key", "value", new RecordOffset(0, 2));
            var kafkaMessage3 = new KafkaMessage<string, string>("key", "value", new RecordOffset(0, 3));
            await commitState.EnqueueMessageAsync(kafkaMessage1);
            await commitState.EnqueueMessageAsync(kafkaMessage2);
            await commitState.EnqueueMessageAsync(kafkaMessage3);
            var mc = new MessageCommitter<string, string>(
                consumer.Object,
                commitState,
                logger.Object,
                TimeSpan.FromDays(1),
                default);
            kafkaMessage1.WasHandled = true;
            kafkaMessage2.WasHandled = true;
            kafkaMessage3.WasHandled = true;

            // when
            await mc.CommitNow();
            mc.Complete();
            await mc.Completion;

            // then
            consumer.Verify(c => c.CommitAsync(It.Is<IRecordOffset>(r => r.Equals(kafkaMessage3.Offset))), Times.Once);
            consumer.VerifyNoOtherCalls();

            Assert.Empty(commitState.GetMessagesToCommit());
        }

        public MessageCommitterTests(ITestOutputHelper output)
        {
            this._output = output;
        }
    }
}
