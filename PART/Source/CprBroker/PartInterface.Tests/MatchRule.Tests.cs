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
using CprBroker.Engine.UpdateRules;
using CprBroker.Schemas.Part;
using CprBroker.Data.Part;
using CprBroker.Utilities;

namespace CprBroker.Tests.PartInterface
{
    namespace MatchRuleTests
    {
        class DummyRule<T> : MatchRule<T>
        {
            public T _GetObject = default(T);

            public Func<RegistreringType1, T> _GetObjectM = null;

            public override T GetObject(RegistreringType1 oio)
            {
                if (_GetObjectM != null)
                    return _GetObjectM(oio);
                else
                    return _GetObject;
            }

            public bool _AreCandidates;
            public override bool AreCandidates(T existingObj, T newObj)
            {
                return _AreCandidates;
            }

            public Action<T, T> _UpdateOioFromXmlType;
            public override void UpdateOioFromXmlType(T existingObj, T newObj)
            {
                if (_UpdateOioFromXmlType != null)
                    _UpdateOioFromXmlType(existingObj, newObj);
            }

        }

        [TestFixture]
        public class AllRules
        {
            [Test]
            public void AllRules_Call_NotNullOrEmpty()
            {
                var rules = MatchRule.AllRules();
                Assert.IsNotEmpty(rules);
            }

            [Test]
            public void AllRules_Call_FirstNotNull()
            {
                var rules = MatchRule.AllRules();
                var first = rules.FirstOrDefault();
                Assert.NotNull(first);
            }            
        }

        [TestFixture]
        public class ApplyRules
        {
            [Test]
            public void ApplyRules_Empty_False()
            {
                var dbReg = new PersonRegistration();
                var oioReg = new RegistreringType1();
                dbReg.SetContents(oioReg);
                var ret = MatchRule.ApplyRules(dbReg, oioReg, new MatchRule[0]);
                Assert.False(ret);
            }

            [Test]
            public void ApplyRules_Empty_SameObject()
            {
                var dbReg = new PersonRegistration();
                var oioReg = new RegistreringType1();
                dbReg.SetContents(oioReg);
                var xml1 = Strings.SerializeObject(dbReg);
                var ret = MatchRule.ApplyRules(dbReg, oioReg, new MatchRule[0]);
                var xml2 = Strings.SerializeObject(dbReg);
                Assert.AreEqual(xml1, xml2);
            }

            [Test]
            public void ApplyRules_IrrelevantRules_SameObject()
            {
                var dbReg = new PersonRegistration();
                var oioReg = new RegistreringType1();
                dbReg.SetContents(oioReg);
                var xml1 = Strings.SerializeObject(dbReg);
                var ret = MatchRule.ApplyRules(dbReg, oioReg);
                var xml2 = Strings.SerializeObject(dbReg);
                Assert.AreEqual(xml1, xml2);
            }
        }

        [TestFixture]
        public class UpdateXmlTypeIfPossible
        {
            [Test]
            public void UpdateXmlTypeIfPossible_NoCandidates_False()
            {
                var oioReg0 = new RegistreringType1();
                var oioReg1 = new RegistreringType1();

                var rule = new DummyRule<RegistreringType1> { _AreCandidates = false, _GetObject = null, _UpdateOioFromXmlType = (r1, r2) => r1.AttributListe = new AttributListeType() { } };
                var ret = rule.UpdateXmlTypeIfPossible(oioReg0, oioReg1);
                Assert.False(ret);
            }

            [Test]
            public void UpdateXmlTypeIfPossible_NoCandidates_NoChanges()
            {
                var oioReg = new RegistreringType1();
                var dbReg = new PersonRegistration();
                dbReg.SetContents(oioReg);
                var xml1 = Strings.SerializeObject(dbReg);

                var rule = new DummyRule<RegistreringType1> { _AreCandidates = false, _GetObject = null, _UpdateOioFromXmlType = (r1, r2) => r1.AttributListe = new AttributListeType() { } };
                var ret = rule.UpdateXmlTypeIfPossible(oioReg, oioReg);
                var xml2 = Strings.SerializeObject(dbReg);
                Assert.AreEqual(xml1, xml2);
            }

            [Test]
            public void UpdateXmlTypeIfPossible_Candidates_True()
            {
                var oioReg = new RegistreringType1();

                var rule = new DummyRule<RegistreringType1> { _AreCandidates = true, _GetObject = oioReg, _UpdateOioFromXmlType = (r1, r2) => r1.AttributListe = new AttributListeType() { } };
                var ret = rule.UpdateXmlTypeIfPossible(oioReg, oioReg);
                Assert.True(ret);
            }

            [Test]
            public void UpdateXmlTypeIfPossible_Candidates_MethodCall()
            {
                var oioReg = new RegistreringType1();
                var dbReg = new PersonRegistration();
                dbReg.SetContents(oioReg);
                bool ff = false;
                var rule = new DummyRule<object> { _AreCandidates = true, _GetObject = "", _UpdateOioFromXmlType = (r1, r2) => ff = true };
                var ret = rule.UpdateXmlTypeIfPossible(oioReg, oioReg);
                Assert.True(ff);
            }

            [Test]
            public void UpdateXmlTypeIfPossible_Candidates_DataChanged()
            {
                var oioReg0 = new RegistreringType1();
                var oioReg1 = new RegistreringType1();

                var xml1 = Strings.SerializeObject(oioReg0);

                var rule = new DummyRule<RegistreringType1> { _AreCandidates = true, _GetObjectM = o => o, _UpdateOioFromXmlType = (r1, r2) => r1.AttributListe = new AttributListeType() { } };
                var ret = rule.UpdateXmlTypeIfPossible(oioReg0, oioReg1);
                var xml2 = Strings.SerializeObject(oioReg0);
                Assert.AreNotEqual(xml1, xml2);
            }

            [Test]
            public void UpdateXmlTypeIfPossible_CandidatesWithNullObjects_False()
            {
                var oioReg = new RegistreringType1();
                var dbReg = new PersonRegistration();
                dbReg.SetContents(oioReg);

                var rule = new DummyRule<RegistreringType1> { _AreCandidates = true, _GetObject = null, _UpdateOioFromXmlType = (r1, r2) => r1.AttributListe = new AttributListeType() { } };
                var ret = rule.UpdateXmlTypeIfPossible(oioReg, oioReg);
                Assert.False(ret);
            }

            [Test]
            public void UpdateXmlTypeIfPossible_CandidatesWithNullObjects_SameData()
            {
                var oioReg0 = new RegistreringType1();
                var oioReg1 = new RegistreringType1();
                var xml1 = Strings.SerializeObject(oioReg0);

                var rule = new DummyRule<RegistreringType1> { _AreCandidates = true, _GetObject = null, _UpdateOioFromXmlType = (r1, r2) => r1.AttributListe = new AttributListeType() { } };
                var ret = rule.UpdateXmlTypeIfPossible(oioReg0, oioReg1);
                var xml2 = Strings.SerializeObject(oioReg0);
                Assert.AreEqual(xml1, xml2);
            }

            [Test]
            public void UpdateXmlTypeIfPossible_CandidatesWithOneNullObject_False([Values(0, 1)]int callNumber)
            {
                var oioReg = new RegistreringType1();
                var oioReg2 = new RegistreringType1();
                var oioRegs = new RegistreringType1[] { oioReg, oioReg2 };

                var rule = new DummyRule<RegistreringType1> { _AreCandidates = true, _GetObjectM = (o) => o == oioRegs[callNumber] ? null : o, _UpdateOioFromXmlType = (r1, r2) => r1.AttributListe = new AttributListeType() { } };
                var ret = rule.UpdateXmlTypeIfPossible(oioReg, oioReg2);
                Assert.False(ret);
            }

            [Test]
            public void UpdateXmlTypeIfPossible_CandidatesWithOneNullObject_FalseAndSameData([Values(0, 1)]int callNumber)
            {
                var oioReg = new RegistreringType1();
                var oioReg2 = new RegistreringType1();
                var oioRegs = new RegistreringType1[] { oioReg, oioReg2 };

                var xml1 = Strings.SerializeObject(oioReg);
                var rule = new DummyRule<RegistreringType1> { _AreCandidates = true, _GetObjectM = (o) => o == oioRegs[callNumber] ? null : o, _UpdateOioFromXmlType = (r1, r2) => r1.AttributListe = new AttributListeType() { } };
                var ret = rule.UpdateXmlTypeIfPossible(oioReg, oioReg2);

                var xml2 = Strings.SerializeObject(oioReg);
                Assert.AreEqual(xml1, xml2);
            }
        }

    }
}
