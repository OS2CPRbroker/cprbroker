﻿using CprBroker.DBR;
using CprBroker.Providers.CPRDirect;
using CprBroker.Providers.DPR;
using CprBroker.Schemas.Wrappers;
using CprBroker.Tests.DBR.Properties;
using CprBroker.Utilities;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace CprBroker.Tests.DBR.DiversionComparison
{
    [TestFixture]
    public class DiversionComparisonTest
    {
        static DiversionComparisonTest()
        {
            if (!Directory.Exists(Settings.Default.DprDiversionCacheDirectory))
                Directory.CreateDirectory(Settings.Default.DprDiversionCacheDirectory);

            PartInterface.Utilities.UpdateConnectionString(Settings.Default.CprBrokerConnectionString);
        }

        [TestFixtureSetUp]
        public void InitBrokerContext()
        {
            CprBroker.Engine.BrokerContext.Initialize(CprBroker.Utilities.Constants.BaseApplicationToken.ToString(), "");
        }

        public string GetRealResponse(string request)
        {
            var fileName = string.Format(@"{0}Response.{1}.txt",
                Settings.Default.DprDiversionCacheDirectory,
                request);
            if (File.Exists(fileName))
            {
                return File.ReadAllText(fileName);
            }
            else
            {
                Console.WriteLine("{0}:\"{1}\"", nameof(request), request);

                var inputData = Providers.DPR.Constants.DiversionEncoding.GetBytes(request);

                using (TcpClient client = new TcpClient(Settings.Default.RealDprDiversionAddress, Settings.Default.RealDprDiversionPort))
                {
                    using (NetworkStream stream = client.GetStream())
                    {
                        stream.Write(inputData, 0, inputData.Length);
                        stream.ReadTimeout = 1000;

                        var retData = new Byte[3500];
                        var retBytes = stream.Read(retData, 0, retData.Length);
                        var retText = Providers.DPR.Constants.DiversionEncoding.GetString(retData.Take(retBytes).ToArray());
                        Console.WriteLine("Response:");
                        Console.WriteLine(retText);

                        File.WriteAllText(fileName, retText);
                        return retText;
                    }
                }
            }
        }

        public DiversionRequest ParseRequest(string request)
        {
            var ret = DiversionRequest.Parse(request);

            if (ret is ErrorRequestType)
            {
                //ret = ret;
            }
            else if (ret is ClassicRequestType)
            {
                ret = new ClassicRequestTypeStub(ret.Contents);
            }
            else if (ret is NewRequestType)
            {
                ret = new NewRequestTypeStub(ret.Contents);
            }
            else
            {
                throw new Exception("Unknown request type created");
            }

            return ret;
        }

        public string GetEmulatedResponse(string request, string dprConnectionString)
        {
            var req = ParseRequest(request);
            var resp = req.Process(dprConnectionString);
            return resp.ToString();
        }

        public void Compare(string request, Func<string, string> preprocessor = null)
        {
            if (preprocessor == null)
            {
                preprocessor = (s) => s;
            }


            var realResponse = preprocessor(GetRealResponse(request).Trim());
            var emulatedResponse = preprocessor(GetEmulatedResponse(request, Settings.Default.ImitatedDprConnectionString).Trim());
            Assert.AreEqual(realResponse, emulatedResponse);
        }

        #region CPR Numbers
        public string[] GetCprNumbers(int count)
        {
            var minPnr = decimal.Parse("0103000000");
            using (var context = new DPRDataContext(Settings.Default.RealDprConnectionString))
            {
                return context.ExecuteQuery<decimal>(""
                    + "SELECT t.PNR FROM DTTOTAL t "
                    + "INNER JOIN DTPERS p ON p.PNR=t.PNR "
                    + "WHERE t.PNR > {0} "
                    + "AND (INDLAESDTO IS NULL OR INDLAESDTO < {1}) "
                    + "AND (HENTTYP IS NULL OR HENTTYP IN ({2},{3})) "
                    + "AND (INDLAESPGM IS NULL OR INDLAESPGM = {4}) "
                    + "AND PERAJDTO > {5} "
                    + "ORDER BY t.PNR "
                    ,
                    minPnr,
                    new DateTime(2016, 10, 1),
                    DataRetrievalTypes.Extract, DataRetrievalTypes.Extract2,
                    UpdatingProgram.DprUpdate,
                     199701010000 // 1 Jan 1997
                     
                    )
                    .Take(count)
                    .ToArray()
                    .Select(p => p.ToPnrDecimalString())
                    .ToArray();
            }
        }

        public string[] CprNumbers2 { get { return GetCprNumbers(2); } }
        public string[] CprNumbers5 { get { return GetCprNumbers(5); } }
        public string[] CprNumbers10 { get { return GetCprNumbers(10); } }
        public string[] CprNumbers100 { get { return GetCprNumbers(100); } }
        public string[] CprNumbers400 { get { return GetCprNumbers(400); } }
        #endregion

        [Test]
        public void InvalidRequest_Old12Char(
            [Values("44", "46", "")]string start,
            [ValueSource(nameof(CprNumbers2))]string pnr)
        {
            var request = start + pnr;
            Compare(request);
        }

        [Test]
        public void NonNumericRequest_Old12Char(
            [Values("aa", "bb")]string start,
            [ValueSource(nameof(CprNumbers2))]string pnr)
        {
            var request = start + pnr;
            Compare(request);
        }

        [Test]
        public void RealRequest_Old12Char(
            [Values("1")]string type,
            [Values("0")]string largeData,
            [ValueSource(nameof(CprNumbers5))]string pnr)
        {
            var request = type + largeData + pnr;
            Compare(request);
        }

        public void CompareNewRequest(
            char type,
            char largeData,
            string pnr,
            char responseData,
            Func<string, string> preprocessor = null)
        {
            var request = ""
                + type
                + largeData
                + pnr
                + "MMXIII"
                + "0" // Do not force diversion
                + responseData
                + "" // no user
                ;
            Compare(request, preprocessor);
        }

        [Test]
        public void RealRequest_New40Char_Ingen(
            [Values('1')]char type,
            [Values('0')]char largeData,
            [ValueSource(nameof(CprNumbers5))]string pnr)
        {
            CompareNewRequest(type, largeData, pnr, 'I');
        }

        [Test]
        public void RealRequest_New40Char_Stam(
            [Values('1')]char type,
            [Values('0')]char largeData,
            [ValueSource(nameof(CprNumbers100))]string pnr)
        {
            CompareNewRequest(type, largeData, pnr, 'S', Preprocess<NewResponseBasicDataType>);
        }

        [Test]
        public void RealRequest_New40Char_Udvidet(
            [Values('1')]char type,
            [Values('0')]char largeData,
            [ValueSource(nameof(CprNumbers400))]string pnr)
        {
            CompareNewRequest(type, largeData, pnr, 'U', Preprocess<NewResponseFullDataType>);
        }

        public string Preprocess<T>(string s) where T : Wrapper, new()
        {
            var propertyDefinitions = new T().PropertyDefinitions;

            var values = s.Substring(7).Split(new char[] { ';' }, propertyDefinitions.Length);

            var valuesAndProps = propertyDefinitions
                .Zip(values, (p, v) => new { Prop = p.Item1.ToUpper(), Pos = p.Item2, Len = p.Item3, Value = v });

            var status = valuesAndProps.SingleOrDefault(p => p.Prop == "STATUS")?.Value;
            if (status == null)
                Console.WriteLine(s);

            var excludedProps = new string[] {
                "AJF",
                "MYNKOD",
                "ADRNVN",
                "INDRAP",
                "FOEDMYNHAENSTART",
                "KUNDENR",
                "FARSKABHAENSTART",
                "AEGTEMRK",
                "AEGTEDOK",
                "FARSKABMYNNVN",
                "TIDLKOMNVN",
                "CIVMYN",
                "STILLINGDTO",
                "MORDOK",
                "FARDOK",
                "MORMRK",
                "FARMRK",
                "UDLANDADRDTO",
                "PNRMRKHAENSTART",
                "KONTAKTADR_KOMKOD",
                "STARTDATE_FORALD_46", "STARTDATE_FORALD_35", // real DPR returns yyyy-MM-dd HH:mm:ss or dd-MM-yyyy randomly                        
                "dummy 1293810"
            };

            var excluded90 = new string[] {
                "POSTDISTTXT", "POSTDISTRICT",
                "POSTNR", "POSTCODE",
                "BYNVN",
                "STANDARDADR",
                "KOMKOD",
                "KOMNVN","AKTKOMNVN",
                "VEJKOD",
                "KOMKOD",
                "VEJADRNVN", "STREETNAME",
                "HUSNR", "HOUSENUMBER",
                "ETAGE", "FLOOR",
                "SIDEDOER","DOOR",
                "CONVN","CAREOFNAME",
                "LOKALITET",
                "TILFLYDTO",
                "TILFLYDTOMRK",
                "TILFLYKOMDTO",
                "FRAFLYKOMDTO",
                "FRAFLYKOMKOD"
            };

            var newValues = valuesAndProps
                .Select(p =>
                {
                    var name = p.Prop;
                    var value = p.Value;

                    //if (Regex.Match(value, @"\A0[0-9]*\Z").Success)
                    value = value.TrimStart('0');

                    if (value.Equals("0")) //STATUSHAENSTART is null in the database but emulated data has 0 as value.
                        value = "";

                    if (excludedProps.FirstOrDefault(pName => name.Contains(pName)) != null)
                    {
                        value = "";
                    }

                    if (
                        (name.Contains("START") || name.Contains("DTO"))
                        && value.Length >= 12 && value.EndsWith("99")
                    )
                    {
                        value = value.Substring(0, value.Length - 2) + "00";
                    }

                    if (status == "90" || status == "80")
                    {
                        if (excluded90.Contains(name))
                        {
                            value = "";
                        }
                    }

                    if (name == "PNR_BORN")
                    {
                        var childValues = value.Split(';');
                        for (int iDok = 2; iDok < childValues.Length; iDok += 3)
                        {
                            childValues[iDok] = "";
                        }
                        value = string.Join(";", childValues);
                    }

                    return string.Format("{0}={1}",
                        p.Prop,
                        value);
                }).ToArray();

            var sRet = s.Substring(0, 7) + string.Join(";", newValues);
            return sRet;
        }
    }
}