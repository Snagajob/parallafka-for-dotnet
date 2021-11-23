﻿using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.Extensions.Logging;
using Parallafka.KafkaConsumer;

namespace Parallafka
{
    internal class MessageCommitter<TKey, TValue>
    {
        private readonly IKafkaConsumer<TKey, TValue> _consumer;
        private readonly CommitState<TKey, TValue> _commitState;
        private readonly ILogger _logger;
        private readonly CancellationToken _stopToken;
        private readonly CancellationTokenSource _stopTimer;
        private readonly ActionBlock<int> _commitBlock;

        public MessageCommitter(
            IKafkaConsumer<TKey, TValue> consumer,
            CommitState<TKey, TValue> commitState,
            ILogger logger,
            TimeSpan timerDelay,
            CancellationToken stopToken)
        {
            this._commitBlock = new ActionBlock<int>(_ => GetAndCommitAnyMessages(),
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = 1,
                    BoundedCapacity = 2
                });

            this._consumer = consumer;
            this._commitState = commitState;
            this._logger = logger;
            this._stopToken = stopToken;
            this._stopTimer = new();
            this.Completion =
                Task.WhenAll(
                    this._commitBlock.Completion,
                    Task.Run(() => this.CommitOnTimer(timerDelay)));
        }

        /// <summary>
        /// Indicates that the message committer should complete all possible commits.
        /// Await <see cref="Completion"/> to know when the committer is completed.
        /// </summary>
        public void Complete()
        {
            this._stopTimer.Cancel();
            this._commitBlock.Post(1);
            this._commitBlock.Complete();

            Parallafka<TKey, TValue>.WriteLine("MC Complete() called");
        }

        /// <summary>
        /// A task that when completed indicates the committer is finished processing
        /// </summary>
        public Task Completion { get; }

        /// <summary>
        /// Commits any messages that can be committed
        /// </summary>
        public Task CommitNow()
        {
            this._commitBlock.Post(1);
            return Task.CompletedTask;
        }

        /// <summary>
        /// Commits any messages on a timer delay
        /// </summary>
        /// <param name="delay"></param>
        /// <returns></returns>
        private async Task CommitOnTimer(TimeSpan delay)
        {
            while (!this._stopTimer.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(delay, this._stopTimer.Token);
                }
                catch (OperationCanceledException)
                {
                }

                await CommitNow();
            }
        }

        /// <summary>
        /// Gets any possible messages to commit and commits them
        /// </summary>
        /// <returns></returns>
        private async Task GetAndCommitAnyMessages()
        {
            Parallafka<TKey, TValue>.WriteLine("GetAndCommitAnyMessages start");
            foreach (var message in this._commitState.GetMessagesToCommit())
            {
                await CommitMessage(message);
            }

            Parallafka<TKey, TValue>.WriteLine("GetAndCommitAnyMessages finish");
        }

        private async Task CommitMessage(IKafkaMessage<TKey, TValue> messageToCommit)
        {
            while (!this._stopToken.IsCancellationRequested)
            {
                try
                {
                    Parallafka<TKey, TValue>.WriteLine($"MsgCommitter: committing {messageToCommit.Offset}");

                    // TODO: inject CancelToken for hard-stop strategy?
                    await this._consumer.CommitAsync(messageToCommit.Offset);

                    break;
                }
                catch (Exception e)
                {
                    this._logger.LogError(e, "Error committing offsets");
                    await Task.Delay(99);
                }
            }
        }
    }
}
