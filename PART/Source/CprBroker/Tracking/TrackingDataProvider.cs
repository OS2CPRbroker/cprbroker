﻿using CprBroker.Data.Applications;
using CprBroker.Data.Part;
using CprBroker.DBR;
using CprBroker.Engine;
using CprBroker.EventBroker.Data;
using CprBroker.Providers.CPRDirect;
using CprBroker.Providers.DPR;
using CprBroker.Schemas;
using CprBroker.Utilities.Config;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CprBroker.PartInterface.Tracking
{
    public class TrackingDataProvider : ITrackingDataProvider
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

        public PersonTrack[] GetTrack(Guid[] personUuids, DateTime? fromDate, DateTime? toDate)
        {
            return Operation
                .Get(
                    personUuids.Select(id => id.ToString()).ToArray(),
                    new OperationType.Types[] { OperationType.Types.Read, OperationType.Types.ReadPeriod },
                    fromDate,
                    toDate)
                .Select(kvp => kvp.Item1.ToPersonTrack(kvp.Item2))
                .ToArray();
        }

        public PersonTrack[] GetSubscribers(Guid[] personUuids)
        {
            // Cache for application table
            var applications = new Dictionary<Guid, Application>();
            using (var dataContext = new ApplicationDataContext())
            {
                applications = dataContext.Applications.ToDictionary(a => a.ApplicationId);
                foreach (var app in applications)
                    app.Value.Token = null;
            }
            Func<Guid, ApplicationType> converter = id =>
            {
                if (applications.ContainsKey(id))
                    return applications[id].ToXmlType();
                else
                    return null as ApplicationType;
            };

            var subscriptions = Subscription.GetSubscriptions(personUuids);
            return subscriptions.Select(s => s.Item1.ToPersonSubscribers(s.Item2, converter))
                .ToArray();
        }

        public PersonTrack[] GetStatus(Guid[] personUuids, DateTime? fromDate, DateTime? toDate)
        {
            return GetTrack(personUuids, fromDate, toDate)
                .Zip(
                    GetSubscribers(personUuids),
                    (track, subscriptions) =>
                    {
                        track.Subscribers = subscriptions.Subscribers;
                        return track;
                    })
                .ToArray();
        }

        public PersonIdentifier[] EnumeratePersons(int startIndex = 0, int maxCount = 200)
        {
            using (var partContext = new PartDataContext())
            {
                return partContext.PersonRegistrations
                    .Join(partContext.PersonMappings,
                        pr => pr.UUID, pm => pm.UUID, (pr, pm) => new PersonIdentifier() { UUID = pr.UUID, CprNumber = pm.CprNumber })
                    .OrderBy(pr => pr.UUID)
                    .Distinct()
                    .Skip(startIndex)
                    .Take(maxCount)
                    .ToArray();
            }
        }

        public bool RemovePerson(PersonIdentifier personIdentifier)
        {
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
            foreach (IPutSubscriptionDataProvider prov in dataProviders)
            {
                tasks.Add(
                    taskFactory
                    .StartNew(
                        () =>
                        {
                            BrokerContext.Current = brokerContext;
                            return prov.RemoveSubscription(personIdentifier);
                        }
                    )
                );
            }

            // Extracts
            tasks.Add(
                taskFactory.StartNew(
                    () =>
                    {
                        BrokerContext.Current = brokerContext;
                        Extract.DeletePersonFromAllExtracts(personIdentifier.CprNumber);
                        return true;
                    }
                )
            );

            // Person data
            tasks.Add(
                taskFactory.StartNew(
                    () =>
                    {
                        BrokerContext.Current = brokerContext;
                        CprBroker.Data.Part.Person.Delete(personIdentifier);
                        return true;
                    }
                )
            );

            // Search Cache
            // Deleted using a trigger on PersonRegistration table

            // DBR
            foreach (var dbr in CprBroker.Engine.Queues.Queue.GetQueues<DbrQueue>())
            {
                using (var dbrDataContext = new DPRDataContext(dbr.ConnectionString))
                {
                    tasks.Add(
                        taskFactory.StartNew(
                            () =>
                            {
                                BrokerContext.Current = brokerContext;
                                CprConverter.DeletePersonRecords(personIdentifier.CprNumber, dbrDataContext);
                                dbrDataContext.SubmitChanges();
                                return true;
                            }
                        )
                    );
                }
            }
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
