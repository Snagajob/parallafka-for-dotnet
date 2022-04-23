using System;
using Microsoft.Extensions.Logging;

namespace Parallafka
{
    public class ParallafkaConfig<TKey, TValue> : IParallafkaConfig
    {
        /// <inheritdoc />
        /// <remarks>Defaults to 3</remarks>
        public int MaxDegreeOfParallelism { get; init; } = 3;

        /// <inheritdoc />
        public ILogger Logger { get; init; } =
            LoggerFactory.Create(builder => builder.AddConsole())
                .CreateLogger<IParallafka<TKey, TValue>>();

        /// <inheritdoc />
        /// <remarks>Defaults to 5 seconds</remarks>
        public TimeSpan CommitDelay { get; init; } = TimeSpan.FromSeconds(5);

        /// <inheritdoc />
        public int MaxQueuedMessages { get; init; } = 1_000;
    }
}