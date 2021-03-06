﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CprBroker.Schemas;
using CprBroker.PartInterface.Tracking;
using CprBroker.Engine;
using CprBroker.Engine.Local;
using CprBroker.Engine.Queues;
using CprBroker.Utilities.Config;

namespace CprBroker.Slet
{
    public partial class RemovePersonDataProvider : IRemovePersonDataProvider
    {
        #region IDataProvider members
        public Version Version
        {
            get
            {
                return new Version(CprBroker.Utilities.Constants.Versioning.Major, CprBroker.Utilities.Constants.Versioning.Major);
            }
        }

        public bool IsAlive()
        {
            return true;
        }
        #endregion

        public bool EnqueuePersonForRemoval(PersonIdentifier personIdentifier)
        {
            var cleanupQueue = Queue.GetQueues<CleanupQueue>().First();

            try
            {
                // Create a semaphore to block queue processing while enqueuing person
                var queueSemaphore = Semaphore.Create();

                var removePersonItem = new CleanupQueueItem() { removePersonItem = new RemovePersonItem(personIdentifier, true) };
                cleanupQueue.Enqueue(removePersonItem, queueSemaphore);
                
                queueSemaphore.Signal();

                Admin.LogFormattedSuccess("<{0}>: Successfully enqueued person <{1}> for removal.", this.GetType().Name, personIdentifier.UUID);
                return true;
            }
            catch (Exception e)
            {
                Admin.LogFormattedError("<{0}>: Error enqueuing person <{1}> for removal.", this.GetType().Name, personIdentifier.UUID);
                return false;
            }
        }

        /// <summary>
        /// Should only be called from Event Broker, since the broker context should hold Event Broker credentials to access the subscription database
        /// </summary>
        /// <param name="personIdentifier"></param>
        /// <returns></returns>
        public bool RemovePerson(PersonIdentifier personIdentifier)
        {
            Admin.LogFormattedSuccess("<{0}>: Removing person <{1}>", this.GetType().Name, personIdentifier.UUID);
            var t = RemovePersonAsync(personIdentifier);
            t.Wait();
            return t.Result;
        }

        public async Task<bool> RemovePersonAsync(PersonIdentifier personIdentifier)
        {
            var tasks = new List<Task<bool>>();
            var taskFactory = new TaskFactory();
            var brokerContext = BrokerContext.Current;

            // Subscriptions
            var section = ConfigManager.Current.DataProvidersSection;
            var factory = new DataProviderFactory();
            var dbProviders = factory.ReadDatabaseDataProviders();
            var dataProviders = factory.GetDataProviderList(section, dbProviders, typeof(IPutSubscriptionDataProvider), SourceUsageOrder.LocalThenExternal);
            tasks.AddRange(
                dataProviders
                .Select(p =>
                    RemoveSubscription(brokerContext, p as IPutSubscriptionDataProvider, personIdentifier)
                )
            );

            // Extracts
            tasks.Add(
                DeletePersonFromAllExtracts(brokerContext, personIdentifier)
            );

            // Person data
            tasks.Add(
                DeletePerson(brokerContext, personIdentifier)
            );

            // Search Cache
            tasks.Add
            (
                DeletePersonFromSearchCache(brokerContext, personIdentifier)
            );

            // DBR
            tasks.Add(
                this.DeletePersonFromAllDBR(brokerContext, personIdentifier)
                );

            // Subscriptions
            tasks.Add(
                this.DeletePersonFromSubscriptions(brokerContext, personIdentifier)
                );

            try
            {
                // Wait for sub tasks to complete
                return Array.TrueForAll(
                    await Task.WhenAll(tasks.ToArray()),
                    b => b);
            }
            catch (AggregateException agrEx)
            {
                // Log the main exception and all sub exceptions
                Engine.Local.Admin.LogException(agrEx);
                foreach (var ex in agrEx.InnerExceptions)
                {
                    Engine.Local.Admin.LogException(ex);
                }
                return false;
            }
        }
    }
}
