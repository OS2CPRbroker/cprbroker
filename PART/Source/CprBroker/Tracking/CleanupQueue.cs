using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CprBroker.Engine.Queues;
using CprBroker.Data.Queues;
using CprBroker.Schemas;
using CprBroker.Engine;
using CprBroker.Providers.Local.Search;
using CprBroker.Engine.Local;
using CprBroker.Slet;
using System.Threading;

namespace CprBroker.PartInterface.Tracking
{
    public class CleanupQueue : Engine.Queues.Queue<CleanupQueueItem>
    {
        public const int QueueTypeId = 400;

        public override CleanupQueueItem[] Process(CleanupQueueItem[] items)
        {
            var prov = new TrackingDataProvider();
            var uuids = items.Select(i => i.removePersonItem.PersonUuid).ToArray();

            var brokerContext = BrokerContext.Current;

            var tasks = items
                .Select(
                (queueItem) => Task.Factory.StartNew(new Func<CleanupQueueItem>(() => ProcessItemWithMutex(brokerContext, prov, queueItem)))
                );

            return Task.WhenAll(tasks)
                .Result
                .Where(r => r != null)
                .ToArray();
        }

        public virtual CleanupQueueItem ProcessItemWithMutex(BrokerContext brokerContext, TrackingDataProvider prov, CleanupQueueItem queueItem)
        {
            BrokerContext.Current = brokerContext;
            Mutex personMutex = null;

            try
            {
                // Establish a person based critical section
                personMutex = new Mutex(false, Utilities.Strings.GuidToString(queueItem.removePersonItem.PersonUuid));
                personMutex.WaitOne();

                // Now the person is locked, all possible usage has been recorded                
                var fromDate = DateTime.Now - SettingsUtilities.MaxInactivePeriod;
                var dbrFromDate = fromDate + SettingsUtilities.DprEmulationRemovalAllowance;
                var excludedMunicipalityCodes = SettingsUtilities.ExcludedMunicipalityCodes;

                return ProcessItem(brokerContext, prov, queueItem, fromDate, dbrFromDate, excludedMunicipalityCodes);
            }
            catch (Exception ex)
            {
                Admin.LogException(ex);
                return null;
            }
            finally
            {
                // Release the lock
                if (personMutex != null)
                    personMutex.ReleaseMutex();
            }
        }

        public virtual CleanupQueueItem ProcessItem(BrokerContext brokerContext, TrackingDataProvider prov, CleanupQueueItem queueItem, DateTime fromDate, DateTime dbrFromDate, int[] excludedMunicipalityCodes)
        {
            BrokerContext.Current = brokerContext;
            var personIdentifier = queueItem.removePersonItem.ToPersonIdentifier();

            // First, make and log the decision to remove the person
            PersonRemovalDecision decision;
            if (queueItem.removePersonItem.forceRemoval)
            {
                decision = PersonRemovalDecision.RemoveCompletely;
                Admin.LogFormattedSuccess("<{0}>: Manually flagging person <{1}> for complete removal,", this.GetType().Name, personIdentifier.UUID);
            }
            else
            {
                decision = prov.GetRemovalDecision(personIdentifier, fromDate, dbrFromDate, excludedMunicipalityCodes);
            }

            // Action time
            // Remove the person if needed
            switch (decision)
            {
                case PersonRemovalDecision.RemoveCompletely:
                    Admin.LogFormattedSuccess("<{0}>: Attempting to remove unused person <{1}>", this.GetType().Name, personIdentifier.UUID);
                    var task1 = new RemovePersonDataProvider().RemovePersonAsync(personIdentifier);
                    task1.Wait();
                    var personRemoved = task1.Result;
                    if (personRemoved)
                    {
                        Admin.LogFormattedSuccess("<{0}>: Succesfully removed unused person <{1}>", this.GetType().Name, personIdentifier.UUID);
                        return queueItem;
                    }
                    else
                    {
                        Admin.LogFormattedError("<{0}>: Failed to remove unused person <{1}>", this.GetType().Name, personIdentifier.UUID);
                        return null;
                    }

                case PersonRemovalDecision.RemoveFromDprEmulation: // Only remove from DPR emulation                    
                    Admin.LogFormattedSuccess("<{0}>:Attempting to remove semi-unused person <{1}> from DPR Emulation", this.GetType().Name, personIdentifier.UUID);
                    var task2 = new RemovePersonDataProvider().DeletePersonFromAllDBR(brokerContext, personIdentifier);
                    task2.Wait();
                    var dbrRemoved = task2.Result;
                    if (dbrRemoved)
                    {
                        Admin.LogFormattedSuccess("<{0}>: Succesfully removed unused person <{1}> from DPR Emulation", this.GetType().Name, personIdentifier.UUID);
                        return queueItem;
                    }         
                    else
                    {
                        Admin.LogFormattedError("<{0}>: Failed to remove semi-unused person <{1}> from DPR Emulation", this.GetType().Name, personIdentifier.UUID);
                        return null;
                    }

                case PersonRemovalDecision.DoNotRemoveDueToExclusion:
                    return queueItem;

                case PersonRemovalDecision.DoNotRemoveDueToUsage:
                    return queueItem;

                default:
                    throw new Exception(string.Format("Unknown value <{0}> for type <{1}>", decision, typeof(PersonRemovalDecision).Name));
            }
        }
    }
}
