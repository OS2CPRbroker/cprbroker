﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CprBroker.Schemas.Part;

namespace CprBroker.Providers.KMD
{
    public static class Utilities
    {
        public static DateTime? ToDateTime(string str)
        {
            DateTime ret;
            if (DateTime.TryParseExact(str, "yyyyMMdd", null, System.Globalization.DateTimeStyles.None, out ret))
            {
                return ret;
            }
            return null;
        }

        public static Schemas.Part.PersonGenderCodeType ToPartGender(string cprNumber)
        {
            int cprNum = int.Parse(cprNumber[cprNumber.Length - 1].ToString());
            if (cprNum % 2 == 0)
            {
                return Schemas.Part.PersonGenderCodeType.female;
            }
            else
            {
                return Schemas.Part.PersonGenderCodeType.male;
            }
        }

        public static decimal GetCivilRegistrationStatus(string kmdStatus, string cprStatus)
        {
            int iKmd = int.Parse(kmdStatus);
            int iCpr = int.Parse(cprStatus);
            if (iKmd == 1)// >10
            {
                return iCpr * 10;
            }
            else
            {
                // TODO: differentiate betwee 01, 03, 05 & 07 because cprStatus is always 0 here
                return 1;
            }
        }

        public static CivilStatusKode ToPartMaritalStatus(char code)
        {
            switch (char.ToUpper(code))
            {
                case 'U':
                    return CivilStatusKode.Ugift;
                case 'G':
                    return CivilStatusKode.Gift;
                case 'F':
                    return CivilStatusKode.Skilt;
                case 'D':
                    // TODO: No deceased status id PART standard
                    throw new Exception("Unhandled civil status");
                case 'E':
                    return CivilStatusKode.Enke;
                case 'P':
                    return CivilStatusKode.RegistreretPartner;
                case 'O':
                    return CivilStatusKode.OphaevetPartnerskab;
                case 'L':
                default:
                    return CivilStatusKode.Laengstlevende;
                // TODO: When to use CivilStatusKode.Separeret?

            }
        }

        public static DateTime? GetMaxDate(params string[] candidateEffectDates)
        {
            return candidateEffectDates
               .Select(s => Utilities.ToDateTime(s))
               .Where(d => d.HasValue)
               .OrderByDescending(d => d.Value)
               .FirstOrDefault();
        }

        //TODO: Remove this method
        public static char FromPartGender(Schemas.Part.Enums.Gender? gender)
        {
            switch (gender)
            {
                case Schemas.Part.Enums.Gender.Male:
                    return 'M';
                    break;
                case Schemas.Part.Enums.Gender.Female:
                    return 'K';
                    break;
                default:
                    return '*';
                    break;
            }
        }

    }
}
