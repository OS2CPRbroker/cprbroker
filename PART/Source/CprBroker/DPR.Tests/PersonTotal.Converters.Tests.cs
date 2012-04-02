﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using CprBroker.Providers.DPR;
using CprBroker.Utilities;
using CprBroker.Utilities.ConsoleApps;
using CprBroker.Schemas;
using CprBroker.Schemas.Part;

namespace CprBroker.Tests.DPR.PersonTotalTests
{
    public class Converters : BaseTests
    {
        [TestFixture]
        public class ToChurchMembershipIndicator
        {
            [Test]
            public void ToChurchMembershipIndicator_F_ReturnsTrue(
                [Values('F', 'f')]char? churchStatus)
            {
                var personTotal = new PersonTotalStub() { ChristianMark = churchStatus };
                var result = personTotal.ToChurchMembershipIndicator();
                Assert.True(result);
            }

            [Test]
            public void ToChurchMembershipIndicator_NonF_ReturnsFalse(
                [Values('A', 'a', 'U', 'u', 'M', 'm', 'S', 's')]char? churchStatus)
            {
                var personTotal = new PersonTotalStub() { ChristianMark = churchStatus };
                var result = personTotal.ToChurchMembershipIndicator();
                Assert.False(result);
            }

            [Test]
            public void ToChurchMembershipIndicator_OtherValues_ReturnsFalse(
                [Values(null, 'w', '2', 'w', 'p')]char? churchStatus)
            {
                var personTotal = new PersonTotalStub() { ChristianMark = churchStatus };
                var result = personTotal.ToChurchMembershipIndicator();
                Assert.False(result);
            }
        }


        [TestFixture]
        public class ToCivilStatusCodeType
        {
            [Test]
            public void ToCivilStatusCodeType_U_NotMarried(
                [Values('u', 'U')]char maritalStatus)
            {
                var personTotal = new PersonTotalStub() { MaritalStatus = maritalStatus };
                var result = personTotal.ToCivilStatusCodeType();
                Assert.AreEqual(CivilStatusKodeType.Ugift, result);
            }

            [Test]
            public void ToCivilStatusCodeType_G_Married(
                [Values('g', 'G')]char maritalStatus)
            {
                var personTotal = new PersonTotalStub() { MaritalStatus = maritalStatus };
                var result = personTotal.ToCivilStatusCodeType();
                Assert.AreEqual(CivilStatusKodeType.Gift, result);
            }

            [Test]
            public void ToCivilStatusCodeType_SeparatedG_ReturnsSeparated(
                [Values('g', 'G')]char maritalStatus)
            {
                var personTotal = new PersonTotalStub() { MaritalStatus = maritalStatus };
                var result = personTotal.ToCivilStatusCodeType();
                Assert.AreEqual(CivilStatusKodeType.Separeret, result);
            }

            [Test]
            public void ToCivilStatusCodeType_F_Divorced(
                [Values('f', 'F')]char maritalStatus)
            {
                var personTotal = new PersonTotalStub() { MaritalStatus = maritalStatus };
                var result = personTotal.ToCivilStatusCodeType();
                Assert.AreEqual(CivilStatusKodeType.Skilt, result);
            }

            [Test]
            public void ToCivilStatusCodeType_E_Widow(
                [Values('e', 'E')]char maritalStatus)
            {
                var personTotal = new PersonTotalStub() { MaritalStatus = maritalStatus };
                var result = personTotal.ToCivilStatusCodeType();
                Assert.AreEqual(CivilStatusKodeType.Enke, result);
            }

            [Test]
            public void ToCivilStatusCodeType_P_RegPartner(
                [Values('p', 'P')]char maritalStatus)
            {
                var personTotal = new PersonTotalStub() { MaritalStatus = maritalStatus };
                var result = personTotal.ToCivilStatusCodeType();
                Assert.AreEqual(CivilStatusKodeType.RegistreretPartner, result);
            }

            [Test]
            public void ToCivilStatusCodeType_U_AbolitionOfRegisteredPartnership(
                [Values('o', 'O')]char maritalStatus)
            {
                var personTotal = new PersonTotalStub() { MaritalStatus = maritalStatus };
                var result = personTotal.ToCivilStatusCodeType();
                Assert.AreEqual(CivilStatusKodeType.OphaevetPartnerskab, result);
            }

            [Test]
            public void ToCivilStatusCodeType_L_LongestLivingPartner(
                [Values('l', 'L')]char maritalStatus)
            {
                var personTotal = new PersonTotalStub() { MaritalStatus = maritalStatus };
                var result = personTotal.ToCivilStatusCodeType();
                Assert.AreEqual(CivilStatusKodeType.Laengstlevende, result);
            }

            [Test]
            public void ToCivilStatusCodeType_D_DeadIsNotMarried(
                [Values('d', 'D')]char maritalStatus)
            {
                var personTotal = new PersonTotalStub() { MaritalStatus = maritalStatus };
                var result = personTotal.ToCivilStatusCodeType();
                Assert.AreEqual(CivilStatusKodeType.Ugift, result);
            }

            [Test]
            [ExpectedException]
            public void ToCivilStatusCodeType_OtherValues_ThrowsException(
                [Values(null, '1', 'q', 'A', 'T', 'w')]char? maritalStatus)
            {
                var personTotal = new PersonTotalStub() { MaritalStatus = maritalStatus };
                personTotal.ToCivilStatusCodeType();
            }



        }



    }
}
