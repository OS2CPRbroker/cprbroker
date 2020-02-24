using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CprBroker.Engine;
using CprBroker.Schemas;
using CprBroker.DBR;
using CprBroker.Providers.CPRDirect;
using CprBroker.Providers.DPR;
using CprBroker.Data.Events;
using CprBroker.Providers.Local.Search;

namespace CprBroker.Slet
{
    partial class RemovePersonDataProvider
    {
        public async Task<bool> RemoveSubscription(BrokerContext brokerContext, IPutSubscriptionDataProvider prov, PersonIdentifier personIdentifier)
        {
            BrokerContext.Current = brokerContext;
            bool result;
            if (prov.IsSharingSubscriptions)
            {
                // Shared subsciptions should not be deleted because they can cause other systems to stop getting updates
                return true;
            }
            else
            {
                result = prov.RemoveSubscription(personIdentifier);
                if (!result)
                {
                    Engine.Local.Admin.LogError(string.Format(
                        "RemovePersonDataProvider.RemoveSubscription() failed removing subscription for <{0}> at data provider <{1}>",
                        personIdentifier.UUID,
                        prov.GetType())
                        );
                }
                return result;
            }
        }

        public async Task<bool> DeletePersonFromAllExtracts(BrokerContext brokerContext, PersonIdentifier personIdentifier)
        {
            BrokerContext.Current = brokerContext;
            Extract.DeletePersonFromAllExtracts(personIdentifier.CprNumber);
            return true;
        }

        public async Task<bool> DeletePerson(BrokerContext brokerContext, PersonIdentifier personIdentifier)
        {
            BrokerContext.Current = brokerContext;
            CprBroker.Data.Part.Person.Delete(personIdentifier);
            return true;
        }

        public async Task<bool> DeletePersonFromSearchCache(BrokerContext brokerContext, PersonIdentifier personIdentifier)
        {
            BrokerContext.Current = brokerContext;
            LocalSearchDataProvider.DeleteFromSearchCache(personIdentifier);
            return true;
        }

        public async Task<bool> DeletePersonFromAllDBR(BrokerContext brokerContext, PersonIdentifier personIdentifier)
        {
            BrokerContext.Current = brokerContext;
            var dbrQueues = CprBroker.Engine.Queues.Queue.GetQueues<DbrQueue>();
            var tasks = dbrQueues
            .Select(q =>
                DeletePersonFromDBR(brokerContext, q, personIdentifier)
            );
            var ret = Array.TrueForAll(
                    await Task.WhenAll(tasks.ToArray()),
                    b => b);
            return ret;
        }

        public async Task<bool> DeletePersonFromDBR(BrokerContext brokerContext, DbrQueue dbr, PersonIdentifier personIdentifier)
        {
            using (var dbrDataContext = new DPRDataContext(dbr.ConnectionString))
            {
                BrokerContext.Current = brokerContext;
                CprConverter.DeletePersonRecords(personIdentifier.CprNumber, dbrDataContext);
                dbrDataContext.SubmitChanges();
                return true;
            }
        }

        public async Task<bool> DeletePersonFromDataChangeEvents(BrokerContext brokerContext, PersonIdentifier personIdentifier)
        {
            BrokerContext.Current = brokerContext;
            DataChangeEvent.DeletePerson(personIdentifier.UUID.Value);
            return true;
        }

        public async Task<bool> DeletePersonFromSubscriptions(BrokerContext brokerContext, PersonIdentifier personIdentifier)
        {
            BrokerContext.Current = brokerContext;
            EventBroker.Data.Subscription.RemovePersonFromAllSubscriptionsAndEvents(personIdentifier.UUID.Value);
            return true;
        }
    }
}
