using System;
using System.Collections.Generic;
using Confluent.Kafka;

namespace MessageFlow.Kafka
{
    public class OffsetCommitCoordinator
    {
        private long _minOffset;
        private readonly Action<Offset> _commitAction;
        private readonly HashSet<long> _processedOffsets = new HashSet<long>();
        
        private readonly object _lock = new object();
        
        public OffsetCommitCoordinator(long minOffset, Action<Offset> commitAction)
        {
            _minOffset = minOffset;
            _commitAction = commitAction;
        }
        
        public void Acknowledge(TopicPartitionOffset offset)
        {
            lock (_lock)
            {
                _processedOffsets.Add(offset.Offset);

                if (_minOffset != offset.Offset)
                {
                    return;
                }
            
                while (_processedOffsets.Contains(_minOffset))
                {
                    _processedOffsets.Remove(_minOffset);
                    _minOffset++;
                }
            
                _commitAction(_minOffset);
            }
        }
    }
}