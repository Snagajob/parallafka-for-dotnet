using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Parallafka.KafkaConsumer;
using Microsoft.Extensions.Logging;
using System.Linq;
using System.Collections.Generic;

namespace Parallafka
{
    public class Parallafka<TKey, TValue> : IParallafka<TKey, TValue>
    {
        private readonly IKafkaConsumer<TKey, TValue> _consumer;
        private readonly IParallafkaConfig _config;
        private readonly ILogger _logger;
        private Func<object> _getStats;

        public static Action<string> WriteLine { get; set; } = (string s) => { };

        public Parallafka(
            IKafkaConsumer<TKey, TValue> consumer,
            IParallafkaConfig config)
        {
            this._consumer = consumer;
            this._config = config;
            this._logger = config.Logger;

            // TODO: Configurable caps, good defaults.
        }

        public object GetStats()
        {
            var gs = _getStats;
            if (gs == null)
            {
                return new { };
            }

            return gs();
        }

        /// <inheritdoc />
        public async Task ConsumeAsync(
            Func<IKafkaMessage<TKey, TValue>, Task> messageHandlerAsync,
            CancellationToken stopToken)
        {
            /* Init pipeline. Pass in a func for what to do in a rebalance.
            ** It can entail extracting the leftover consumed pre-commit messages to be fed in after restart.
            ** The rebalance callback should then request a pipeline shutdown, giving ~10 seconds to finish some ready commits.
            ** After that point, it should rapidly halt everything and be disposed.
            ** Once we're sure of that, we can start a new pipeline, this time with the front of the received queue
            ** being the salvaged queue of uncommitted messages from the prior incarnation.
            ** The consumer instance remains unchanged, so we MUST feed these into the new pipeline first
            ** for the retained partitions before newly polled messages enter.
            */

            var runtime = Stopwatch.StartNew();

            CancellationToken userStopToken = stopToken;

            // This can be assigned during rebalances so we can feed them back into the top of the new pipeline.
            IEnumerable<KafkaMessageWrapped<TKey, TValue>> uncommittedMessages = Enumerable.Empty<KafkaMessageWrapped<TKey, TValue>>();

            BufferBlock<KafkaMessageWrapped<TKey, TValue>> polledMessages = new(
                new DataflowBlockOptions()
                {
                    BoundedCapacity = 50
                });
            Task pollerThread = this.KafkaPollerThread(polledMessages, userStopToken);

            while (!userStopToken.IsCancellationRequested)
            {
                CancellationTokenSource pipelineStop = new();
                userStopToken.Register(() => pipelineStop.Cancel());

                Source<KafkaMessageWrapped<TKey, TValue>> poller = new(polledMessages);

                BufferBlock<KafkaMessageWrapped<TKey, TValue>> uncommittedMessageBlock = new();
                foreach (var message in uncommittedMessages)
                {
                    uncommittedMessageBlock.Post(message);
                }
                CompleteOnEmptyBufferSource<KafkaMessageWrapped<TKey, TValue>> uncommittedMessagesFromPriorPipeline =
                    new(uncommittedMessageBlock);

                SerialMultiSource<KafkaMessageWrapped<TKey, TValue>> messageSource = new(
                    sourcesInOrder: new ISource<KafkaMessageWrapped<TKey, TValue>>[]
                    {
                        uncommittedMessagesFromPriorPipeline,
                        poller
                    });

                pipelineStop.Token.Register(() => messageSource.Complete());
                // we actually need to keep any 'salvaged' messages in the queue for next pipeline.
                // Is this a difficult RC opportunity? Where do we put the unfinished messages in the pipeline
                // WRT those in the source?

                Pipeline processor = new(
                    config: this._config,
                    consumer: this._consumer,
                    messageSource: messageSource,
                    messageHandlerAsync: messageHandlerAsync,
                    onPartitionsRevoked: (partitions, processor) =>
                    {
                        Parallafka<string, string>.WriteLine(
                            this._consumer.ToString() + "REVOKING PARTITIONS !!! " + string.Join(", ", partitions.Select(p => p.Partition)));

                        pipelineStop.Cancel();
                    },
                    logger: this._logger);

                this._getStats = () => new
                {
                    ConsumeCallDuration = runtime.Elapsed.ToString("g"),
                    ConsumeCallDurationMs = runtime.ElapsedMilliseconds,
                    Pipeline = processor.GetStats(),
                };

                await processor.ProcessAsync(pipelineStop.Token);
                if (userStopToken.IsCancellationRequested)
                {
                    break;
                }

                await messageSource.Completion;
                Parallafka<string, string>.WriteLine("Pipeline has stopped due to rebalance. Will restart");
                // Pipeline has stopped. Salvage the uncommitted messages and prepare the new message source.
                // uncommittedMessages = uncommittedMessagesFromPriorPipeline.Items.Concat(processor.GetUncommittedMessages());
            }

            Parallafka<string, string>.WriteLine("Consume finished");
        }

        private class Pipeline
        {
            private readonly IParallafkaConfig _config;

            private readonly IKafkaConsumer<TKey, TValue> _consumer;

            private readonly ISource<KafkaMessageWrapped<TKey, TValue>> _messageSource;

            private readonly Func<IKafkaMessage<TKey, TValue>, Task> _messageHandlerAsync;

            private readonly Action<IReadOnlyCollection<TopicPartition>, Pipeline> _onPartitionsRevoked;

            private readonly ILogger _logger;

            private readonly CancellationToken _stopToken;

            private Func<object> _getStats;

            public Pipeline(
                IParallafkaConfig config,
                IKafkaConsumer<TKey, TValue> consumer,
                ISource<KafkaMessageWrapped<TKey, TValue>> messageSource,
                Func<IKafkaMessage<TKey, TValue>, Task> messageHandlerAsync,
                Action<IReadOnlyCollection<TopicPartition>, Pipeline> onPartitionsRevoked,
                ILogger logger)
            {
                this._config = config;
                this._consumer = consumer;
                this._messageSource = messageSource;
                this._messageHandlerAsync = messageHandlerAsync;
                this._onPartitionsRevoked = onPartitionsRevoked;
                this._logger = logger;
            }

            public async Task ProcessAsync(CancellationToken stopToken)
            {
                int maxQueuedMessages = this._config.MaxQueuedMessages;
                using var localStop = new CancellationTokenSource();
                var localStopToken = localStop.Token;

                var commitState = new CommitState<TKey, TValue>(
                    maxQueuedMessages,
                    localStopToken);

                var messagesByKey = new MessagesByKey<TKey, TValue>(stopToken);

                // the message router ensures messages are handled by key in order
                var router = new MessageRouter<TKey, TValue>(commitState, messagesByKey, stopToken);
                var routingTarget = new ActionBlock<KafkaMessageWrapped<TKey, TValue>>(router.RouteMessage,
                    new ExecutionDataflowBlockOptions
                    {
                        BoundedCapacity = 1,
                        MaxDegreeOfParallelism = 1
                    });

                var finishedRouter = new MessageFinishedRouter<TKey, TValue>(messagesByKey);

                // Messages eligible for handler threads to pick up and handle.
                var handler = new MessageHandler<TKey, TValue>(
                    this._messageHandlerAsync,
                    this._logger,
                    localStopToken);
                var handlerTarget = new ActionBlock<KafkaMessageWrapped<TKey, TValue>>(handler.HandleMessage,
                    new ExecutionDataflowBlockOptions
                    {
                        BoundedCapacity = Math.Max(maxQueuedMessages, this._config.MaxDegreeOfParallelism),
                        MaxDegreeOfParallelism = this._config.MaxDegreeOfParallelism
                    });

                // This is where messages go after being handled: enqueued to be committed when it's safe.
                var committer = new MessageCommitter<TKey, TValue>(
                    this._consumer,
                    commitState, 
                    this._logger);

                var commitPoller = new CommitPoller(committer);

                commitState.OnMessageQueueFull += (sender, args) =>
                {
                    commitPoller.CommitNow();
                };

                router.MessagesToHandle.LinkTo(handlerTarget);
                finishedRouter.MessagesToHandle.LinkTo(handlerTarget);

                // handled messages are sent to both:
                // . the finished router (send the next message for the key)
                // . the committer
                var messageHandledTarget = new ActionBlock<KafkaMessageWrapped<TKey, TValue>>(
                    m =>
                    {
                        commitPoller.CommitWithin(this._config.CommitDelay);
                        return finishedRouter.MessageHandlerFinished(m);
                    },
                    new ExecutionDataflowBlockOptions
                    {
                        BoundedCapacity = 1000
                    });
                handler.MessageHandled.LinkTo(messageHandledTarget);

                this._messageSource.Block.LinkTo(routingTarget);

                // this._consumer.AddPartitionsRevokedHandler(partitions =>
                // {
                //     Parallafka<string, string>.WriteLine(this._consumer.ToString() + "REVOKING PARTITIONS !!! " + string.Join(", ", partitions.Select(p => p.Partition)));

                //     // Tell committer to flush, w/ timeout. Commit as much as possible before giving up partitions in rebalance.
                //     // Might need to do that, get as far as possible, then shut everything down, then restart all the processors,
                //     // and have the initial source of records be maybe what's in the CommitState, uncommitted, excluding revoked partitions.

                //     // 1. Refactor to make clean restarts easy.
                //     // 2. Refactor to make the initial source for the routingTarget entrypoint any uncommitted messages in queues, before polling again.
                //     //      Maybe we should just set the uncommittedMessagesByPartition directly.
                //     // 3. Have this handler trigger the restart.


                //     // CancellationTokenSource cancelCommit = new(13000);
                //     // try
                //     // {
                //     //     committer.CommitNow(cancelCommit.Token).Wait();
                //     // }
                //     // catch (OperationCanceledException)
                //     // {
                //     //     Parallafka<string, string>.WriteLine("Pre-restart commit timed out");
                //     // }

                //     Parallafka<string, string>.WriteLine("Stopping everything to reset");

                //     // Flush the pipes of records with one of these partitions.
                //     var revokedPartitions = partitions.Select(p => p.Partition).ToArray();
                //     // commitState.PurgeRecordsFromPartitions(revokedPartitions);
                //     // messagesByKey.PurgeRecordsFromPartitions(revokedPartitions);
                //     // messagesByKey.PurgeRecordsFromPartitions(revokedPartitions);
                //     // router.PurgeRecordsFromPartitions(revokedPartitions);
                // });

                var state = "Polling for Kafka messages";
                this._getStats = () =>
                    new
                    {
                        ConsumerState = state,
                        MaxQueuedMessages = maxQueuedMessages,

                        CommitState = commitState.GetStats(),
                        MessagesByKey = messagesByKey.GetStats(),
                        Router = router.GetStats(),
                        RoutingTarget = new
                        {
                            routingTarget.InputCount,
                            TaskStatus = routingTarget.Completion.Status.ToString()
                        },
                        FinishedRouter = finishedRouter.GetStats(),
                        Handler = handler.GetStats(),
                        HandlerTarget = new
                        {
                            handlerTarget.InputCount,
                            TaskStatus = handlerTarget.Completion.Status.ToString()
                        },
                        Committer = committer.GetStats(),
                        CommitPoller = commitPoller.GetStats(),
                        MessageHandledTarget = new
                        {
                            messageHandledTarget.InputCount,
                            TaskStatus = messageHandledTarget.Completion.Status.ToString()
                        }
                    };

                await this._messageSource.Completion;

                // done polling, wait for the routingTarget to finish
                state = "Shutdown: Awaiting message routing";
                routingTarget.Complete();
                await routingTarget.Completion;

                // wait for the router to finish (it should already be done)
                state = "Shutdown: Awaiting message handler";
                router.MessagesToHandle.Complete();
                await router.MessagesToHandle.Completion;

                // wait for the finishedRoute to complete handling all the queued messages
                finishedRouter.Complete();
                state = "Shutdown: Awaiting message routing completion";
                await finishedRouter.Completion;

                // wait for the message handler to complete (should already be done)
                state = "Shutdown: Awaiting handler shutdown";
                handlerTarget.Complete();
                await handlerTarget.Completion;

                state = "Shutdown: Awaiting handled shutdown";
                handler.MessageHandled.Complete();
                await handler.MessageHandled.Completion;

                state = "Shutdown: Awaiting handled target shutdown";
                messageHandledTarget.Complete();
                await messageHandledTarget.Completion;

                // wait for the committer to finish
                state = "Shutdown: Awaiting message commit poller";
                commitPoller.Complete();
                await commitPoller.Completion;

                this._getStats = null;

                // commitState should be empty
                WriteLine("ConsumeFinished");
            }

            public object GetStats()
            {
                var gs = _getStats;
                if (gs == null)
                {
                    return new { };
                }

                return gs();
            }
        }

        private async Task<int> KafkaPollerThread(ITargetBlock<KafkaMessageWrapped<TKey, TValue>> routingTarget, CancellationToken stopToken)
        {
            // pass in a routingtarget bufferblock that stays alive until we shut down the whole consume call.
            // so the stoptoken here is the top-level kill-switch.
            int? delay = null;

            int sent = 0;

            try
            {
                for(;;)
                {
                    // TODO: Error handling
                    try
                    {
                        if (delay.HasValue)
                        {
                            await Task.Delay(delay.Value, stopToken);
                            delay = null;
                        }

                        IKafkaMessage<TKey, TValue> message = await this._consumer.PollAsync(stopToken);
                        if (message == null)
                        {
                            stopToken.ThrowIfCancellationRequested();
                            this._logger.LogWarning("Polled a null message while not shutting down: breach of IKafkaConsumer contract");
                            delay = 50;
                        }
                        else
                        {
                            WriteLine($"Poller: Sending {message.Key} {message.Offset}");
                            await routingTarget.SendAsync(message.Wrapped(), stopToken);
                            sent++;
                            WriteLine($"Poller: Sent {message.Key} {message.Offset}");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (Exception e)
                    {
                        this._logger.LogError(e, "Error in Kafka poller thread");
                        delay = 333;
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception e)
            {
                this._logger.LogCritical(e, "Fatal error in Kafka poller thread");
            }

            return sent;
        }
    }

    internal interface ISource<T>
    {
        ISourceBlock<T> Block { get; }

        void Complete();

        Task Completion { get; }
    }

    internal class Source<T> : ISource<T>
    {
        public Source(ISourceBlock<T> block)
        {
            this.Block = block;
        }

        public ISourceBlock<T> Block { get; private set; }

        public Task Completion => this.Block.Completion;

        public void Complete()
        {
            this.Block.Complete();
        }
    }

    internal class CompleteOnEmptyBufferSource<T> : ISource<T>
    {
        private readonly BufferBlock<T> _bufferBlock;
        
        public CompleteOnEmptyBufferSource(BufferBlock<T> bufferBlock)
        {
            this._bufferBlock = bufferBlock;
            this.Completion = this.CompleteWhenEmptyAsync();
        }

        private async Task CompleteWhenEmptyAsync()
        {
            while (!this._bufferBlock.Completion.IsCompleted && this._bufferBlock.Count > 0)
            {
                // TODO
                await Task.Delay(50);
            }
            
            this.Complete();
            await this._bufferBlock.Completion;
        }

        public ISourceBlock<T> Block => this._bufferBlock;

        public Task Completion { get; }

        public void Complete()
        {
            this._bufferBlock.Complete();
        }
    }

    internal class SerialMultiSource<T> : ISource<T>
    {
        private readonly IEnumerable<ISource<T>> _sourcesInOrder;

        private readonly BufferBlock<T> _output;

        public SerialMultiSource(IEnumerable<ISource<T>> sourcesInOrder)
        {
            this._sourcesInOrder = sourcesInOrder;
            this._output = new BufferBlock<T>(
                new DataflowBlockOptions()
                {
                    BoundedCapacity = 1                    
                });
            this.Completion = this.RunAsync();
        }

        public async Task RunAsync()
        {
            foreach (var source in this._sourcesInOrder)
            {
                source.Block.LinkTo(this._output);
                await Task.WhenAny(source.Completion, this._output.Completion);
                if (this._output.Completion.IsCompleted)
                {
                    break;
                }
            }
        }

        public ISourceBlock<T> Block => this._output;

        public Task Completion { get; private set; }

        public void Complete()
        {
            this._output.Complete();
        }
    }
}
