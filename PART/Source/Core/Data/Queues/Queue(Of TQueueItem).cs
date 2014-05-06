﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CprBroker.Data.Queues
{

    public abstract class Queue<TQueueItem>
        where TQueueItem : IQueueItem, new()
    {
        public Queue Impl { get; private set; }


        public Queue(Guid queueId)
        {
            this.Impl = Queue.GetById(queueId);
        }

        public TQueueItem[] GetNext(int maxCount)
        {
            return Impl.GetNext(maxCount)
                .Select(
                (qi) =>
                {
                    var ret = new TQueueItem() { Impl = qi };
                    ret.DeserializeFromKey(qi.ItemKey);
                    return ret;
                })
                .ToArray();
        }

        public void Remove(TQueueItem[] items)
        {
            Impl.Remove(items.Select(i => i.Impl).ToArray());
        }

        public void MarkFailure(TQueueItem[] items)
        {
            Impl.MarkFailure(items.Select(i => i.Impl).ToArray());
        }

        public void Enqueue(TQueueItem item)
        {
            Enqueue(new TQueueItem[] { item });
        }
        public void Enqueue(TQueueItem[] items)
        {
            var itemKeys = items.Select(it => it.SerializeToKey()).ToArray();
            Impl.Enqueue(itemKeys);
        }

        /// <summary>
        /// Implements the actual task that is supposed to be implemented for each queue item
        /// Successful
        /// </summary>
        /// <param name="items">The queue items</param>
        /// <returns>A subset if the input that was processed successfully</returns>
        public abstract TQueueItem[] Process(TQueueItem[] items);

        public void Run()
        {
            var items = GetNext(Impl.BatchSize);
            while (items.FirstOrDefault() != null)
            {
                var succeeded = Process(items);
                Remove(succeeded);

                var failedItems = items.Except(succeeded).ToArray();
                MarkFailure(failedItems);
                items = GetNext(Impl.BatchSize);
            }
        }
    }

}