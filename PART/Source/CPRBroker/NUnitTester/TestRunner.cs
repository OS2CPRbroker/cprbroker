﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CprBroker.NUnitTester
{
    /// <summary>
    /// Contains members that manage the overall running of unit tests
    /// </summary>
    public static class TestRunner
    {

        public static CPRAdministrationWS.CPRAdministrationWS AdminService;
        public static CPRPersonWS.CPRPersonWS PersonService;
        public static Access.Access AccessService;
        public static Part.Part PartService;
        public static Subscriptions.Subscriptions SubscriptionsService;

        public static void Initialize()
        {
            AdminService = new NUnitTester.CPRAdministrationWS.CPRAdministrationWS();
            AdminService.ApplicationHeaderValue = new NUnitTester.CPRAdministrationWS.ApplicationHeader()
            {
                ApplicationToken = TestData.BaseAppToken,
                UserToken = TestData.userToken
            };
            ReplaceServiceUrl(AdminService, SystemType.CprBroker);
            Console.WriteLine(AdminService.Url);


            PersonService = new NUnitTester.CPRPersonWS.CPRPersonWS();
            PersonService.ApplicationHeaderValue = new NUnitTester.CPRPersonWS.ApplicationHeader()
            {
                ApplicationToken = TestData.BaseAppToken,
                UserToken = TestData.userToken
            };
            ReplaceServiceUrl(PersonService, SystemType.CprBroker);
            Console.WriteLine(PersonService.Url);


            PartService = new NUnitTester.Part.Part();
            PartService.ApplicationHeaderValue = new NUnitTester.Part.ApplicationHeader()
            {
                ApplicationToken = TestData.BaseAppToken,
                UserToken = TestData.userToken
            };
            PartService.EffectDateHeaderValue = new NUnitTester.Part.EffectDateHeader() { EffectDate = null };
            PartService.RegistrationDateHeaderValue = new NUnitTester.Part.RegistrationDateHeader() { RegistrationDate = null };
            ReplaceServiceUrl(PartService, SystemType.CprBroker);
            Console.WriteLine(PartService.Url);


            SubscriptionsService = new NUnitTester.Subscriptions.Subscriptions();
            SubscriptionsService.ApplicationHeaderValue = new NUnitTester.Subscriptions.ApplicationHeader()
            {
                ApplicationToken = TestData.BaseAppToken,
                UserToken = TestData.userToken
            };
            ReplaceServiceUrl(SubscriptionsService, SystemType.EventBroker);
            Console.WriteLine(SubscriptionsService.Url);


            AccessService = new NUnitTester.Access.Access();
            ReplaceServiceUrl(AccessService, SystemType.EventBroker);
            Console.WriteLine(AccessService.Url);
        }

        private enum SystemType
        {
            CprBroker,
            EventBroker
        }
        private static void ReplaceServiceUrl(System.Web.Services.Protocols.SoapHttpClientProtocol service, SystemType systemType)
        {
            Uri uri = new Uri(service.Url);
            string hostAndPort = uri.Host;
            if (!uri.IsDefaultPort)
            {
                hostAndPort += ":" + uri.Port;
            }

            if (systemType == SystemType.CprBroker)
            {
                service.Url = service.Url.Replace(hostAndPort, "localhost:1551");
            }
            else
            {
                service.Url = service.Url.Replace(hostAndPort, "localhost:1552");
            }
        }
    }
}
