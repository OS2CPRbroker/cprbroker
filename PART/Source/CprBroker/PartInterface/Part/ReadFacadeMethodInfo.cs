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
using CprBroker.Utilities;
using CprBroker.Data.Applications;

namespace CprBroker.Engine.Part
{
    /// <summary>
    /// Facade method for Read and RefreshRead
    /// </summary>
    public class ReadFacadeMethodInfo : FacadeMethodInfo<ReadSubMethodInfo, LaesOutputType, LaesResultatType>
    {
        protected LaesInputType Input = null;
        SourceUsageOrder LocalAction;
        public QualityLevel? QualityLevel;

        private ReadFacadeMethodInfo()
        { }

        public ReadFacadeMethodInfo(LaesInputType input, SourceUsageOrder localAction, string appToken, string userToken)
            : base(appToken, userToken)
        {
            this.Input = input;
            this.LocalAction = localAction;
        }

        public override StandardReturType ValidateInput()
        {
            if (Input == null)
            {
                return StandardReturType.NullInput();
            }

            if (!Strings.IsGuid(Input.UUID))
            {
                return StandardReturType.InvalidUuid(Input.UUID);
            }

            return StandardReturType.OK();
        }

        public override void Initialize()
        {
            SubMethodInfos = new ReadSubMethodInfo[]
            {
                new ReadSubMethodInfo(Input, LocalAction)
            };
        }

        public override OperationType.Types? MainOperationType
        {
            get
            {
                return OperationType.Types.Read;
            }
        }

        public override string[] InputOperationKeys
        {
            get
            {
                return new string[] { Strings.GuidToString(Input.UUID) };
            }
        }

        public override LaesResultatType Aggregate(object[] results)
        {
            var laesResultat = new LaesResultatType()
            {
                Item = results[0]
            };

            CityNameMapping.ApplyIfNeeded(laesResultat);
            if (laesResultat.Item is RegistreringType1)
            {
                if (CprBroker.Utilities.Config.ConfigManager.Current.Settings.TrimFutureInReadOperation)
                    (laesResultat.Item as RegistreringType1).TrimFuture();

                (laesResultat.Item as RegistreringType1).OrderByStartDate(
                    !CprBroker.Utilities.Config.ConfigManager.Current.Settings.CprDirectReturnsNewestFirst);
            }
            QualityLevel = (SubMethodInfos[0] as ReadSubMethodInfo).QualityLevel;
            return laesResultat;
        }
    }
}
