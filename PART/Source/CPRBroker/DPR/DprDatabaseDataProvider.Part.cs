﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CprBroker.Engine;
using CprBroker.Schemas;
using CprBroker.Schemas.Part;
using CprBroker.Schemas.Util;
using System.Linq.Expressions;


namespace CprBroker.Providers.DPR
{
    public partial class DprDatabaseDataProvider : ClientDataProvider, IPartReadDataProvider
    {
        public static readonly Guid ActorId = new Guid("{4A953CF9-B4C1-4ce9-BF09-2BF655DC61C7}");

        [Obsolete]
        protected override string TestResponse
        {
            get
            {
                return "11471926741444Rkzmqvyjtyt                             Jpgdu                                                                               Sgk S?wuscdsxnbg                        579D485671    3744Btjaoxuwbsoxr       2005197001";
            }
        }

        #region IPartReadDataProvider Members

        public RegistreringType1 Read(PersonIdentifier uuid, LaesInputType input, Func<string, Guid> cpr2uuidFunc, out QualityLevel? ql)
        {
            CprBroker.Schemas.Part.RegistreringType1 ret = null;
            EnsurePersonDataExists(uuid.CprNumber);


            //TODO: Get values from input
            DateTime? effectDate = null;
            if (!effectDate.HasValue)
            {
                effectDate = DateTime.Today;
            }
            using (var dataContext = new DPRDataContext(this.DatabaseObject.ConnectionString))
            {
                var db =
                (
                    from personInfo in PersonInfo.PersonInfoExpression.Compile()(effectDate.Value, dataContext)
                    where personInfo.PersonName.PNR == Decimal.Parse(uuid.CprNumber)
                    select personInfo
                ).ToList();

                var populated = PersonInfo2.Populate(db, input.VirkningFraFilter.ToDateTime(), input.VirkningTilFilter.ToDateTime());
                var targetRegistration = populated.FirstOrDefault();
                if (targetRegistration != null)
                {
                    ret = targetRegistration.ToRegisteringType1(effectDate, cpr2uuidFunc, dataContext);
                    //ret.ActorId = ActorId;
                }
            }
            ql = QualityLevel.DataProvider;
            return ret;
        }

        public CprBroker.Schemas.Part.PersonRegistration[] List(PersonIdentifier[] uuids, DateTime? effectDate, out QualityLevel? ql)
        {
            // TODO: Add DPR List implementation after Read implementation is OK
            throw new NotImplementedException();
        }

        #endregion
    }
}
