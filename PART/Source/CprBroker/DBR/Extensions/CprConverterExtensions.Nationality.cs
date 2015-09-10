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
 * Dennis Amdi Skov Isaksen
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
using CprBroker.Providers.DPR;
using CprBroker.Providers.CPRDirect;
using CprBroker.Schemas.Part;

namespace CprBroker.DBR.Extensions
{
    public static partial class CprConverterExtensions
    {
        public static Nationality ToDpr(this CurrentCitizenshipType currentCitizenship)
        {
            Nationality n = new Nationality();
            n.PNR = Decimal.Parse(currentCitizenship.PNR);
            n.CprUpdateDate = CprBroker.Utilities.Dates.DateToDecimal(currentCitizenship.Registration.RegistrationDate, 12);
            n.CountryCode = currentCitizenship.CountryCode;

            n.NationalityStartDate = currentCitizenship.CitizenshipStartDateDecimal;

            n.NationalityEndDate = null; // This is the current nationality
            n.CorrectionMarker = null; //This is the current status
            return n;
        }

        public static Nationality ToDpr(this HistoricalCitizenshipType historicalCitizenship)
        {
            Nationality n = new Nationality();
            n.PNR = Decimal.Parse(historicalCitizenship.PNR);
            n.CprUpdateDate = CprBroker.Utilities.Dates.DateToDecimal(historicalCitizenship.Registration.RegistrationDate, 12);
            n.CountryCode = historicalCitizenship.CountryCode;

            if (historicalCitizenship.CitizenshipStartDate.HasValue)
                n.NationalityStartDate = CprBroker.Utilities.Dates.DateToDecimal(historicalCitizenship.CitizenshipStartDate.Value, 12);

            if (historicalCitizenship.CitizenshipEndDate.HasValue)
                n.NationalityEndDate = CprBroker.Utilities.Dates.DateToDecimal(historicalCitizenship.CitizenshipEndDate.Value, 12);

            n.CorrectionMarker = historicalCitizenship.CorrectionMarker;
            return n;
        }

    }
}
