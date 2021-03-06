﻿using CprBroker.Engine;
using CprBroker.PartInterface.Tracking;
using CprBroker.Schemas;
using CprBroker.Tests.PartInterface;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CprBroker.Tests.Tracking
{
    namespace TrackingDataProviderTests
    {
        [TestFixture]
        public class GetRemovalDecision : TestBase
        {
            class TrackingDataProviderStub : TrackingDataProvider
            {
                public bool _PersonHasSubscribers = false;
                public override bool PersonHasSubscribers(Guid personUuid)
                {
                    return _PersonHasSubscribers;
                }

                public List<DateTime> _PersonUsageDates = new List<DateTime>();
                public override bool PersonHasUsage(Guid personUuid, DateTime? fromDate, DateTime? toDate)
                {
                    if (fromDate == null)
                        fromDate = DateTime.MinValue;
                    if (toDate == null)
                        toDate = DateTime.MaxValue;

                    return _PersonUsageDates.Exists(d => d >= fromDate && d <= toDate);
                }

                public bool _PersonLivesInExcludedMunicipality = false;
                public override bool PersonLivesInExcludedMunicipality(PersonIdentifier personIdentifier, int[] excludedMunicipalities)
                {
                    return _PersonLivesInExcludedMunicipality;
                }
            }

            [SetUp]
            public void InitBrokerContext()
            {
                Utilities.InitBrokerContext();
            }

            PersonRemovalDecision GetDecision(TrackingDataProvider prov, PersonIdentifier pId)
            {
                return prov.GetRemovalDecision(
                    pId,
                    DateTime.Now - SettingsUtilities.MaxInactivePeriod,
                    DateTime.Now - SettingsUtilities.MaxInactivePeriod + SettingsUtilities.DprEmulationRemovalAllowance,
                    SettingsUtilities.ExcludedMunicipalityCodes
                    );
            }

            [Test]
            public void GetRemovalDecision_None_RemoveCompletely()
            {
                var prov = new TrackingDataProviderStub();
                var ret = GetDecision(prov, Utilities.NewPersonIdentifier());
                Assert.AreEqual(PersonRemovalDecision.RemoveCompletely, ret);
            }

            [Test]
            public void GetRemovalDecision_OldUsage_RemoveCompletely()
            {
                var pId = Utilities.NewPersonIdentifier();
                var prov = new TrackingDataProviderStub();
                prov._PersonUsageDates.Add(DateTime.Now.AddYears(-1));
                var ret = GetDecision(prov, pId);
                Assert.AreEqual(PersonRemovalDecision.RemoveCompletely, ret);
            }

            [Test]
            public void GetRemovalDecision_ExcludedMunicipality_Excluded(
                [Values(true, false)]bool hasUsage,
                [Values(true, false)]bool hasSubscriptions)
            {
                var pId = Utilities.NewPersonIdentifier();
                var prov = new TrackingDataProviderStub();
                prov._PersonLivesInExcludedMunicipality = true;
                if (hasSubscriptions)
                    prov._PersonHasSubscribers = true;
                if (hasUsage)
                    prov._PersonUsageDates.Add(DateTime.Now.AddHours(-1));

                var ret = GetDecision(prov, pId);
                Assert.AreEqual(PersonRemovalDecision.DoNotRemoveDueToExclusion, ret);
            }


            [Test]
            public void GetRemovalDecision_AlmostOldUsage_NoSubscription_RemoveDprEmulation()
            {
                var pId = Utilities.NewPersonIdentifier();
                var prov = new TrackingDataProviderStub();
                prov._PersonUsageDates.Add(
                    DateTime.Now -
                    SettingsUtilities.MaxInactivePeriod +
                    TimeSpan.FromSeconds(SettingsUtilities.DprEmulationRemovalAllowance.TotalSeconds / 2));

                var ret = GetDecision(prov, pId);
                Assert.AreEqual(PersonRemovalDecision.RemoveFromDprEmulation, ret);
            }

            [Test]
            public void GetRemovalDecision_AlmostOldUsage_Subscription_DoNotRemove()
            {
                var pId = Utilities.NewPersonIdentifier();
                var prov = new TrackingDataProviderStub();
                prov._PersonHasSubscribers = true;
                prov._PersonUsageDates.Add(
                    DateTime.Now -
                    SettingsUtilities.MaxInactivePeriod +
                    TimeSpan.FromSeconds(SettingsUtilities.DprEmulationRemovalAllowance.TotalSeconds / 2));

                var ret = GetDecision(prov, pId);
                Assert.AreEqual(PersonRemovalDecision.DoNotRemoveDueToUsage, ret);
            }

            [Test]
            public void GetRemovalDecision_NewUsage_DoNotRemove(
                [Values(true, false)]bool hasSubscriptions)
            {
                var pId = Utilities.NewPersonIdentifier();
                var prov = new TrackingDataProviderStub();
                if (hasSubscriptions)
                    prov._PersonHasSubscribers = true;
                prov._PersonUsageDates.Add(DateTime.Now.AddHours(-1));
                var ret = GetDecision(prov, pId);
                Assert.AreEqual(PersonRemovalDecision.DoNotRemoveDueToUsage, ret);
            }

            [Test]
            public void GetRemovalDecision_Subscriptions_DoNotRemove(
                [Values(true, false)]bool hasUsage)
            {
                var pId = Utilities.NewPersonIdentifier();
                var prov = new TrackingDataProviderStub();
                prov._PersonHasSubscribers = true;
                if (hasUsage)
                    prov._PersonUsageDates.Add(DateTime.Now.AddHours(-1));
                var ret = GetDecision(prov, pId);
                Assert.AreEqual(PersonRemovalDecision.DoNotRemoveDueToUsage, ret);

            }
        }


    }
}
