﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CprBroker.Providers.CPRDirect;
using NUnit.Framework;

namespace CprBroker.Tests.CPRDirect
{
    [TestFixture]
    public class WrapperTests
    {
        class WrapperStub : Wrapper
        {
            public int _Length;
            public override int Length
            {
                get { return _Length; }
            }
        }

        #region Contents

        [Test]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void Contents_InvalidLength_ThrowsException(
            [Random(0, 10000, 5)]int len,
            [Values(1, 30, -12)]int diff)
        {
            var w = new WrapperStub() { _Length = len };
            w.Contents = new string('s', len + diff);
        }

        [Test]
        public void Contents_InvalidLength_ThrowsException(
            [Random(0, 10000, 5)]int len)
        {
            var w = new WrapperStub() { _Length = len };
            var val = new string('s', len);
            w.Contents = val;
            Assert.AreEqual(val, w.Contents);
        }

        #endregion

        class WrapperStub1 : Wrapper
        {
            public override int Length
            {
                get { return 6; }
            }
        }
        class WrapperStub2 : Wrapper
        {
            public override int Length
            {
                get { return 6; }
            }
        }

        class ParentWrapperStub : Wrapper
        {
            public override int Length
            {
                get { return 12; }
            }
            [MinMaxOccurs(0, 1)]
            public WrapperStub1 WrapperStub1 = null;

            [MinMaxOccurs(0, 1000)]
            public List<WrapperStub2> WrapperStub2 = new List<WrapperStub2>();
        }

        [TestFixture]
        public class Parse
        {
            Dictionary<string, Type> WrapperMap;

            [SetUp]
            public void Initialize()
            {
                WrapperMap = new Dictionary<string, Type>();
                WrapperMap["001"] = typeof(WrapperStub1);
                WrapperMap["002"] = typeof(WrapperStub2);
            }

            [Test]
            [ExpectedException]
            public void Parse_RandomCode_Exception()
            {
                string data = Guid.NewGuid().ToString();
                var wrappers = Wrapper.Parse(data, new Dictionary<string, Type>());
                Assert.IsEmpty(wrappers);
            }
            [Test]
            public void Parse_Normal_CorrectCount()
            {
                string data = "001AAA002BBB";

                var wrappers = Wrapper.Parse(data, WrapperMap);
                Assert.AreEqual(2, wrappers.Count);
            }

            [Test]
            [ExpectedException]
            public void Parse_TooShortString_Exception()
            {
                string data = "001";
                Wrapper.Parse(data, WrapperMap);
            }

            [Test]
            [ExpectedException]
            public void Parse_TooLongString_Exception()
            {
                string data = "001" + Guid.NewGuid().ToString();
                Wrapper.Parse(data, WrapperMap);
            }

            Random Random = new Random();
            [Test]
            public void Parse_MixedCount_CorrectCount(
                [Random(10, 200, 5)]int w1Count,
                [Random(10, 200, 5)]int w2Count)
            {
                List<bool> arr = new List<bool>(new bool[w1Count + w2Count]);
                for (int i = 0; i < w1Count; i++)
                {
                    arr[i] = true;
                }
                for (int i = 0; i < w2Count; i++)
                {
                    arr[i + w1Count] = false;
                }
                string data = "";
                while (arr.Count > 0)
                {
                    var index = Random.Next(0, arr.Count);
                    string code = (arr[index]) ? "001" : "002";
                    data += code + Guid.NewGuid().ToString().Substring(0, 3);
                    arr.RemoveAt(index);
                }
                var wrappers = Wrapper.Parse(data, WrapperMap);
                Assert.AreEqual(w1Count, wrappers.Where(w => w is WrapperStub1).Count());
                Assert.AreEqual(w2Count, wrappers.Where(w => w is WrapperStub2).Count());
            }
        }

        [TestFixture]
        public class FillFrom
        {
            [Test]
            public void FillFrom_Zero_OK()
            {
                var wrappers = new Wrapper[0];
                new ParentWrapperStub().FillFrom(wrappers);
            }
            [Test]
            [ExpectedException]
            public void FillFrom_TwoForSinglePosition_Exception()
            {
                var wrappers = new Wrapper[] { new WrapperStub1(), new WrapperStub1() };
                new ParentWrapperStub().FillFrom(wrappers);
            }

            [Test]
            public void FillFrom_Multiple_CorrectCounts(
                [Random(10, 200, 5)]int w2Count)
            {
                var wrappers = new List<Wrapper>();
                wrappers.Add(new WrapperStub1());
                for (int i = 0; i < w2Count; i++)
                {
                    wrappers.Add(new WrapperStub2());
                }
                var par = new ParentWrapperStub();
                par.FillFrom(wrappers);
                Assert.NotNull(par.WrapperStub1);
                Assert.AreEqual(w2Count, par.WrapperStub2.Count);
                Assert.AreEqual(w2Count, par.WrapperStub2.Where(w => w is WrapperStub2).Count());
            }
        }
    }
}