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

        /// <summary>
        /// For testing. Must be set by the time ConsumeAsync is called.
        /// </summary>
        internal Func<KafkaMessageWrapped<TKey, TValue>, Task> OnMessageCommitted { get; set; }

        /// <summary>
        /// For testing.
        /// </summary>
        //internal Action OnCommitQueueFull { get; set; } = () => {}; // doesn't work

        internal int SharedKeyMessagesQueuedCount => this._currentProcessor?.SharedKeyMessagesQueuedCount ?? 0;

        private SemaphoreSlim _pollerLock = new(initialCount: 1);

        private Pipeline _currentProcessor;

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
                    BoundedCapacity = 1
                });
            Task pollerThread = this.KafkaPollerThread(routingTarget: polledMessages, stopToken: userStopToken);

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
                // Can the consumer freak out and reset?

                // ***********
                // Any issues with the offsets? What if it resets and poller resets - what does the ordered to-commit queue look like?
                // ***********

                // And what about RCs with the partitions being revoked without noticing, like during a restart?
                // Like what if they're added back.
                // We could pause the poller or something and purge...

                IReadOnlyCollection<TopicPartition> partitionsAssigned = null;

                this._consumer.AddPartitionsAssignedHandler(partitions =>
                {
                    if (partitions.Count == 0)
                    {
                        Parallafka<string, string>.WriteLine("PartitionsAssigned is empty");
                        //return; // twas this
                    }
                    Parallafka<string, string>.WriteLine($"Partitions assigned: " + string.Join(", ", partitions.Select(p => p.Partition)));
                    partitionsAssigned = partitions;
                    // int nStaleRecordsRemoved = 0;
                    // int nBufferedOriginally = polledMessages.Count;
                    // while (polledMessages.TryReceive(m => !partitionsAssigned.Any(pa => m.Offset.Partition == pa.Partition), out var message))
                    // {
                    //     Parallafka<string, string>.WriteLine("A revoked partition message was buffered. Removed it.");
                    //     nStaleRecordsRemoved++;
                    // }
                    // Parallafka<string, string>.WriteLine($"Removed {nStaleRecordsRemoved} stale records out of {nBufferedOriginally} buffered");
                });

                // TODO: don't double-register the revokedHandler.

                var partitionsAssignedBeforeRebalance = partitionsAssigned;

                this._currentProcessor = new Pipeline(
                    config: this._config,
                    consumer: this._consumer,
                    messageSource: messageSource,
                    messageHandlerAsync: async m =>
                    {
                        if (partitionsAssigned != null)
                        {
                            if (!partitionsAssigned.Any(p => p.Partition == m.Offset.Partition))
                            {
                                Parallafka<string, string>.WriteLine($"Attempting to process a message with revoked partition! {m.Offset.Partition}");
                            }
                        }
                        await messageHandlerAsync(m);
                    },
                    onPartitionsRevoked: (partitions, processor) =>
                    {
                        partitionsAssignedBeforeRebalance = partitionsAssigned;

                        Parallafka<string, string>.WriteLine(
                            this._consumer.ToString() + "REVOKING PARTITIONS yo !!! " + string.Join(", ", partitions.Select(p => p.Partition)));

                        Parallafka<string, string>.WriteLine("messages buffered from poller: " + polledMessages.Count);

                        processor.InitiateShutdown(delayToFinishUpBeforeHardStop: TimeSpan.FromSeconds(10)); // catch it here, see what happens in processor

                        Parallafka<string, string>.WriteLine("Waiting for processor to shut down");
                        processor.Completion.Wait();
                    },
                    onMessageCommitted: this.OnMessageCommitted,
                    logger: this._logger,
                    onCommitQueueFull: () => { /* TODO: remove */});

                this._getStats = () => new
                {
                    ConsumeCallDuration = runtime.Elapsed.ToString("g"),
                    ConsumeCallDurationMs = runtime.ElapsedMilliseconds,
                    Pipeline = this._currentProcessor.GetStats(),
                };

                await this._currentProcessor.ProcessAsync(pipelineStop.Token);
                if (userStopToken.IsCancellationRequested)
                {
                    break;
                }

                await messageSource.Completion;
                Parallafka<string, string>.WriteLine("Pipeline has stopped due to rebalance. Will restart");

                if (partitionsAssigned == null)
                {
                    throw new Exception("partitionsAssigned is null");
                }

                while (partitionsAssigned.Count > 0 && partitionsAssigned == partitionsAssignedBeforeRebalance) // TODO
                {
                    Parallafka<string, string>.WriteLine("Waiting for new assignment"); // and for purge
                    await Task.Delay(50);
                }
                
                //string oldPts = string.Join(", ", partitionsAssignedBeforeRebalance.Select(p => p.Partition));
                //string newPts = string.Join(", ", partitionsAssigned.Select(p => p.Partition));
                Parallafka<string, string>.WriteLine($"Done waiting for new assignment.");
                

                // await this._pollerLock.WaitAsync();

                // while (polledMessages.TryReceive(m => !partitionsAssigned.Any(pa => m.Offset.Partition == pa.Partition), out var message))
                // {
                //     Parallafka<string, string>.WriteLine("A revoked partition message was buffered. Removed it.");
                // }
                // this._pollerLock.Release();

                // This should be done with the poller paused. ^
                // Maybe it can be done in OnPartitionsAssigned since that's called via Poll().

                // if (partitionsRevoked == null)
                // {
                //     throw new Exception("PartitionsRevoked should not be null");
                // }

                // int nReceived = 0;
                // while (polledMessages.TryReceive(message => partitionsRevoked.Any(pr => pr.Partition == message.Offset.Partition), out var message))
                // {
                //     Parallafka<string, string>.WriteLine("A revoked partition message was buffered. Removed it.");
                //     nReceived++;
                // }
                // if (nReceived > 1)
                // {
                //     throw new Exception($"Got {nReceived} instead of max expected of 1");
                // }

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

            private readonly Func<KafkaMessageWrapped<TKey, TValue>, Task> _onMessageCommitted;

            private readonly ILogger _logger;

            private readonly CancellationToken _stopToken;

            private Func<object> _getStats;

            private readonly TaskCompletionSource _completionSource = new();

            private readonly CancellationTokenSource _stopTokenSource = new();

            private TimeSpan? _delayToFinishUpBeforeHardStop = TimeSpan.FromSeconds(10);

            public Task Completion => this._completionSource.Task;

            public int SharedKeyMessagesQueuedCount => this._messagesByKey?.SharedKeyMessagesQueuedCount ?? 0;

            private readonly Action _onCommitQueueFull;

            private MessagesByKey<TKey, TValue> _messagesByKey;

            public Pipeline(
                IParallafkaConfig config,
                IKafkaConsumer<TKey, TValue> consumer,
                ISource<KafkaMessageWrapped<TKey, TValue>> messageSource,
                Func<IKafkaMessage<TKey, TValue>, Task> messageHandlerAsync,
                Action<IReadOnlyCollection<TopicPartition>, Pipeline> onPartitionsRevoked,
                Func<KafkaMessageWrapped<TKey, TValue>, Task> onMessageCommitted,
                ILogger logger,
                Action onCommitQueueFull = null)
            {
                this._config = config;
                this._consumer = consumer;
                this._messageSource = messageSource; // TODO: Need to salvage what's in the "salvaged" part of this on restart.
                this._messageHandlerAsync = messageHandlerAsync;
                this._onPartitionsRevoked = onPartitionsRevoked;
                this._onMessageCommitted = onMessageCommitted;
                this._logger = logger;
                this._onCommitQueueFull = onCommitQueueFull;
            }

            public void InitiateShutdown(TimeSpan delayToFinishUpBeforeHardStop)
            {
                this._delayToFinishUpBeforeHardStop = delayToFinishUpBeforeHardStop;
                this._stopTokenSource.Cancel();
            }

            public async Task ProcessAsync(CancellationToken stopToken)
            {
                var userStopToken = stopToken;
                userStopToken.Register(() => this._stopTokenSource.Cancel());
                CancellationTokenSource hardStopper = new();
                this._stopTokenSource.Token.Register(() =>
                {
                    // Turn off the message source first. Then, after the configured "finish up" delay, hard-stop the downstream pipeline.
                    this._messageSource.Complete();
                    hardStopper.CancelAfter(
                        Convert.ToInt32(this._delayToFinishUpBeforeHardStop?.TotalMilliseconds ?? 0));
                });

                int maxQueuedMessages = this._config.MaxQueuedMessages;

                var commitState = new CommitState<TKey, TValue>(
                    maxQueuedMessages,
                    hardStopper.Token);

                var messagesByKey = this._messagesByKey = new MessagesByKey<TKey, TValue>(hardStopper.Token);

                // the message router ensures messages are handled by key in order
                var router = new MessageRouter<TKey, TValue>(commitState, messagesByKey, hardStopper.Token);
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
                    hardStopper.Token);
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
                    this._logger,
                    this._onMessageCommitted);

                var commitPoller = new CommitPoller(committer);

                commitState.OnMessageQueueFull += (sender, args) =>
                {
                    this._onCommitQueueFull?.Invoke();
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

                this._consumer.AddPartitionsRevokedHandler(partitions =>
                {
                    this._onPartitionsRevoked(partitions, this);
                });

                this._messageSource.Block.LinkTo(routingTarget);

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
                WriteLine("Pipeline finished");

                this._completionSource.TrySetResult();
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

                        await this._pollerLock.WaitAsync(stopToken);

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
                    finally
                    {
                        this._pollerLock.Release();
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
