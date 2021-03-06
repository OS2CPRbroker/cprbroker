﻿/* ***** BEGIN LICENSE BLOCK *****
 * Version: MPL 1.1/GPL 2.0/LGPL 2.1
 *
 * The contents of this file are subject to the Mozilla Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS"basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The CPR Broker concept was initally developed by
 * Gentofte Kommune / Municipality of Gentofte, Denmark.
 * Contributor(s):
 * Steen Deth
 *
 *
 * The Initial Code for CPR Broker and related components is made in
 * cooperation between Magenta, Gentofte Kommune and IT- og Telestyrelsen /
 * Danish National IT and Telecom Agency
 *
 * Contributor(s):
 * Beemen Beshara
 *
 * The code is currently governed by IT- og Telestyrelsen / Danish National
 * IT and Telecom Agency
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the MPL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the MPL, the GPL or the LGPL.
 *
 * ***** END LICENSE BLOCK ***** */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using CprBroker.EventBroker.Data;
using CprBroker.Data.Part;
using CprBroker.Tests.PartInterface;

namespace CprBroker.Tests.EventBroker
{

    namespace SubscriptionTests
    {
        [TestFixture]
        public class GetDataChangeEventMatches : TestBase
        {
            public static string[] MunicipalityCodes = new string[] { "851", "500", "322" };
            [Test]
            public void UpdatePersonLists_Matching_Found([ValueSource("MunicipalityCodes")]string municipalityCode)
            {

                Guid uuid = Guid.NewGuid();
                Guid personRegId = Guid.NewGuid();

                var pers = Utils.CreatePerson(uuid);
                var dbReg = Utils.CreatePersonRegistration(pers, municipalityCode, personRegId, uuid);

                using (var dataContext = new EventBrokerDataContext(EventDatabase.ConnectionString))
                {
                    var subscription = AddSubscription(dataContext, Utils.CreateSoegObject(municipalityCode), false, true, SubscriptionType.SubscriptionTypes.DataChange);
                    var dataChangeEvent = AddChanges(dataContext, false, dbReg);
                    dataContext.SubmitChanges();

                    using (var cprDataContext = new PartDataContext())
                    {
                        cprDataContext.Persons.InsertOnSubmit(pers);
                        cprDataContext.PersonRegistrations.InsertOnSubmit(dbReg);
                        cprDataContext.SubmitChanges();
                    }

                    var sss = subscription.Matches(dataChangeEvent).ToArray();
                    Assert.IsNotEmpty(sss);
                }
            }

            [Test]
            public void UpdatePersonLists_NotMatching_NotFound([ValueSource("MunicipalityCodes")]string municipalityCode)
            {
                Guid uuid = Guid.NewGuid();
                Guid personRegId = Guid.NewGuid();

                var pers = Utils.CreatePerson(uuid);
                var dbReg = Utils.CreatePersonRegistration(pers, municipalityCode, personRegId, uuid);


                using (var dataContext = new EventBrokerDataContext(EventDatabase.ConnectionString))
                {
                    var subscription = AddSubscription(dataContext, Utils.CreateSoegObject(municipalityCode + "222"), false, true, SubscriptionType.SubscriptionTypes.DataChange);
                    var dataChangeEvent = AddChanges(dataContext, false, dbReg);
                    dataContext.SubmitChanges();

                    using (var cprDataContext = new PartDataContext())
                    {
                        cprDataContext.Persons.InsertOnSubmit(pers);
                        cprDataContext.PersonRegistrations.InsertOnSubmit(dbReg);
                        cprDataContext.SubmitChanges();
                    }

                    var sss = subscription.Matches(dataChangeEvent).ToArray();
                    Assert.IsEmpty(sss);
                }
            }
        }
    }
}
