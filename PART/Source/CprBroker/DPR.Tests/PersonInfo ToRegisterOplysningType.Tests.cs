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
 * Niels Elgaard Larsen
 * Leif Lodahl
 * Steen Deth
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
using CprBroker.Providers.DPR;
using CprBroker.Utilities;
using CprBroker.Utilities.ConsoleApps;
using CprBroker.Schemas;
using CprBroker.Schemas.Part;

namespace CprBroker.Tests.DPR
{
    [TestFixture]
    class PersonInfoConversionTests : BaseTests
    {
        class PersonInfoStub : PersonInfo
        { }

        class DprDatabaseDataProviderStub : DprDatabaseDataProvider
        {
            public DprDatabaseDataProviderStub()
            {
                this.ConfigurationProperties = new Dictionary<string, string>();
                this.ReturnForeignersAsCprBorgerType = false;
            }
        }

        #region ToRegisterOplysningType

        [Test]
        public void ToRegisterOplysningType_DanishNationality_ReturnsCprBorger(
            [Values(5100)]decimal nationality,
            [ValueSource("RandomCprNumbers5")]decimal cprNumber,
            [Values(true, false)] bool alwaysReturnCprBorger)
        {
            var personInfo = new PersonInfo()
            {
                PersonTotal = new PersonTotalStub() { PNR = cprNumber, Nationality = nationality.ToDecimalString() },
                Nationality = new Nationality() { CountryCode = nationality }
            };
            var result = personInfo.ToRegisterOplysningType(new DprDatabaseDataProviderStub() { ReturnForeignersAsCprBorgerType = alwaysReturnCprBorger });
            Assert.IsInstanceOf<CprBorgerType>(result.Item);
        }


        [Test]
        public void ToRegisterOplysningType_SpecialNationality_ReturnsUkendt(
            [Values(5001, 5103)]decimal nationality,
            [ValueSource("RandomCprNumbers5")]decimal cprNumber)
        {
            var personInfo = new PersonInfo()
            {
                PersonTotal = new PersonTotalStub() { PNR = cprNumber, Nationality = nationality.ToDecimalString() },
                Nationality = new Nationality() { CountryCode = nationality }
            };
            var result = personInfo.ToRegisterOplysningType(new DprDatabaseDataProviderStub() { ReturnForeignersAsCprBorgerType = false });
            Assert.IsInstanceOf<UkendtBorgerType>(result.Item);
        }

        [Test]
        public void ToRegisterOplysningType_SpecialNationality_ReturnsCprBorger(
            [Values(5001, 5103)]decimal nationality,
            [ValueSource("RandomCprNumbers5")]decimal cprNumber)
        {
            var personInfo = new PersonInfo()
            {
                PersonTotal = new PersonTotalStub() { PNR = cprNumber, Nationality = nationality.ToDecimalString() },
                Nationality = new Nationality() { CountryCode = nationality }
            };
            var result = personInfo.ToRegisterOplysningType(new DprDatabaseDataProviderStub() { ReturnForeignersAsCprBorgerType = true });
            Assert.IsInstanceOf<CprBorgerType>(result.Item);
        }

        [Test]
        public void ToRegisterOplysningType_ForeignNationality_ReturnsUdenlandsBorger(
            [Values(5150, 856, 12, 99)]decimal nationality,
            [ValueSource("RandomCprNumbers5")]decimal cprNumber)
        {
            var personInfo = new PersonInfo()
            {
                PersonTotal = new PersonTotalStub() { PNR = cprNumber, Nationality = nationality.ToDecimalString() },
                Nationality = new Nationality() { CountryCode = nationality }
            };
            var result = personInfo.ToRegisterOplysningType(new DprDatabaseDataProviderStub() { ReturnForeignersAsCprBorgerType = false });
            Assert.IsInstanceOf<UdenlandskBorgerType>(result.Item);
        }

        [Test]
        public void ToRegisterOplysningType_ForeignNationality_ReturnsCprBorger(
            [Values(5150, 856, 12, 99)]decimal nationality,
            [ValueSource("RandomCprNumbers5")]decimal cprNumber)
        {
            var personInfo = new PersonInfo()
            {
                PersonTotal = new PersonTotalStub() { PNR = cprNumber, Nationality = nationality.ToDecimalString() },
                Nationality = new Nationality() { CountryCode = nationality }
            };
            var result = personInfo.ToRegisterOplysningType(new DprDatabaseDataProviderStub() { ReturnForeignersAsCprBorgerType = true });
            Assert.IsInstanceOf<CprBorgerType>(result.Item);
        }
        #endregion



    }
}
