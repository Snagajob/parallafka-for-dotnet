namespace Parallafka.KafkaConsumer
{
    /// <summary>
    /// Describes the partition/offset of the message
    /// </summary>
    public class TopicPartition
    {
        public string Topic { get; }

        public int Partition { get; }

        public TopicPartition(string topic, int partition)
        {
            this.Topic = topic;
            this.Partition = partition;
        }
    }
}