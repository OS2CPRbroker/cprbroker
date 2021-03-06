﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CprBroker.Engine.Queues;

namespace CprBroker.Engine.Tasks
{
    public class QueueExecutionManager : TaskExecutionManager<QueueExecuter, QueueExecuter.EqualityComparer>
    {
        public override void StartTask(QueueExecuter task)
        {
            CprBroker.Engine.Local.Admin.LogSuccess(string.Format("Staring freshly loaded queue <{0}>", task.Queue.QueueId));
            task.Start();
        }

        public override void DisposeTask(QueueExecuter q)
        {
            q.Stop();
            q.Dispose();
        }

        public override QueueExecuter[] GetTasks()
        {
            return Queue.GetQueues<Queue>()
                .Where(q => q != null)
                .Select(q => new QueueExecuter(q))
                .ToArray();
        }

        public override void RefreshTask(QueueExecuter existingTask, QueueExecuter freshTask)
        {
            existingTask.Queue.Impl = freshTask.Queue.Impl;
        }
    }
}
