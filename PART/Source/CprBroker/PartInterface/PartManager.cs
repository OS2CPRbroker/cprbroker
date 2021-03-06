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
using CprBroker.Schemas;
using CprBroker.Schemas.Part;
using CprBroker.Engine.Part;
using CprBroker.Engine;

namespace CprBroker.PartInterface
{
    /// <summary>
    /// This section implements the PART interface methods as of the PART standard
    /// </summary>
    public partial class PartManager : RequestProcessor
    {
        public LaesOutputType Read(string userToken, string appToken, LaesInputType input, SourceUsageOrder localAction, out QualityLevel? qualityLevel)
        {
            ReadFacadeMethodInfo facadeMethod = new ReadFacadeMethodInfo(input, localAction, appToken, userToken);
            var ret = GetMethodOutput<ReadSubMethodInfo, LaesOutputType, LaesResultatType>(facadeMethod);
            qualityLevel = facadeMethod.QualityLevel;
            return ret;
        }

        public ListOutputType1 List(string userToken, string appToken, ListInputType input, SourceUsageOrder localAction, out QualityLevel? qualityLevel)
        {
            ListOutputType1 ret = null;

            ret = GetMethodOutput<ReadSubMethodInfo, ListOutputType1, LaesResultatType[]>(
                new ListFacadeMethodInfo(input, localAction, appToken, userToken)
                );

            //TODO: remove quality level because it applies to individual elements rather than the whole result
            qualityLevel = QualityLevel.LocalCache;
            return ret;
        }

        public SoegOutputType Search(string userToken, string appToken, SoegInputType1 searchCriteria, out QualityLevel? qualityLevel)
        {
            SearchFacadeMethodInfo facadeMethod = new SearchFacadeMethodInfo(searchCriteria, appToken, userToken);
            var ret = GetMethodOutput<SearchSubMethodInfo, SoegOutputType, string[]>(facadeMethod);
            //TODO: Move into Search method of data provider
            qualityLevel = QualityLevel.LocalCache;
            return ret;
        }

        public SoegListOutputType SearchList(string userToken, string appToken, SoegInputType1 searchCriteria, SourceUsageOrder sourceUsageOrder)
        {
            SearchListFacadeMethodInfo facadeMethod = new SearchListFacadeMethodInfo(searchCriteria, sourceUsageOrder, appToken, userToken);
            var ret = GetMethodOutput<SearchListSubMethodInfo, SoegListOutputType, LaesResultatType[]>(facadeMethod);
            return ret;
        }

        public GetUuidOutputType GetUuid(string userToken, string appToken, string cprNumber)
        {
            var ret = GetUuidArray(userToken, appToken, new string[] { cprNumber });
            return new GetUuidOutputType()
            {
                Item = ret.Item == null ? null : ret.Item.FirstOrDefault(),
                StandardRetur = ret.StandardRetur,
                UUID = ret.UUID == null ? null : ret.UUID.FirstOrDefault()
            };
        }

        public GetUuidArrayOutputType GetUuidArray(string userToken, string appToken, string[] cprNumberArray)
        {
            var facadeMethod = new GetUuidArrayFacadeMethodInfo(cprNumberArray, appToken, userToken);
            return GetBatchMethodOutput<IPartPersonMappingDataProvider, GetUuidArrayOutputType, string, string>(facadeMethod);
        }

        public BasicOutputType<bool> PutSubscription(string userToken, string appToken, Guid[] personUuids)
        {
            var facadeMethod = new PutSubscriptionFacadeMethodInfo(personUuids, appToken, userToken);
            return GetMethodOutput<bool>(facadeMethod);
        }
    }
}
