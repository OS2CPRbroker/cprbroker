﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CprBroker.Schemas;
using CprBroker.Schemas.Part;

namespace CprBroker.Providers.E_M
{
    public partial class Citizen
    {
        private static VirkningType ToVirkningType(Citizen citizen)
        {
            // TODO: validate outcome of this method
            var dates = new List<DateTime?>();
            dates.AddRange(new DateTime?[]
            {           
                Converters.ToDateTime(citizen.Birthdate,citizen.BirthdateUncertainty),
                Converters.ToDateTime(citizen.CitizenStatusTimestamp,citizen.CitizenStatusTimestampUncertainty),                    
                citizen.CpcNationalChurchTimestamp,
                citizen.CprPersonTimestamp,
                Converters.ToDateTime(citizen.DepartureTimestamp,citizen.DepartureTimestampUncertainty),
                Converters.ToDateTime(citizen.DirectoryProtectionEndDate,citizen.DirectoryProtectionRemovalDateUncertainty),
                Converters.ToDateTime(citizen.DiretoryProtectionDate, citizen.DirectoryProtectionDateUncertainty),
                Converters.ToDateTime(citizen.MaritalStatusTimestamp,citizen.MaritalStatusTimestampUncertainty),
                Converters.ToDateTime(citizen.MaritalStatusTerminationTimestamp,citizen.MaritalStatusTerminationTimestampUncertainty),
                Converters.ToDateTime(citizen.MunicipalityArrivalDate,citizen.MunicipalityArrivalTimestampUncertainty),
                Converters.ToDateTime(citizen.NationalChurchMarkerDate, citizen.NationalChurchMarkerUncertainty),
                Converters.ToDateTime(citizen.NationalityChangeTimestamp,citizen.NationalityChangeTimestampUncertainty),
                Converters.ToDateTime(citizen.NationalityTerminationTimestamp,citizen.NationalityTerminationTimestampUncertainty),
                Converters.ToDateTime(citizen.OccupationDate,citizen.OccupationDateUncertainty),
                Converters.ToDateTime(citizen.PNRCreationDate, citizen.PnrCreationdateUncertainty),
                Converters.ToDateTime(citizen.PNRMarkingDate,citizen.PnrMarkingDateUncertainty),
                Converters.ToDateTime(citizen.RelocationTimestamp,citizen.RelocationTimestampUncertainty),
            });
            var maxDate = Converters.GetMaxDate(dates.ToArray());
            return VirkningType.Create(maxDate, null);

        }

        private static TidspunktType ToTidspunktType(Citizen citizen)
        {
            // TODO: Add a more meaningful date to registration date input
            return TidspunktType.Create(Converters.GetMaxDate(
                Converters.ToDateTime(citizen.BirthRegistrationDate, citizen.BirthRegistrationDateUncertainty)
                ));
        }

    }
}