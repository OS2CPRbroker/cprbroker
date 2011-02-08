﻿using System;
using System.Collections.Generic;
using System.Data.Linq;
using System.Linq;
using System.Text;
using CprBroker.Schemas.Part;

namespace CprBroker.DAL.Part
{
    public partial class ForeignCitizenCountry
    {
        public static CountryIdentificationCodeType ToXmlType(ForeignCitizenCountry db)
        {
            if (db != null)
            {
                return CountryRef.ToXmlType(db.CountryRef);
            }
            return null;
        }

        public static CountryIdentificationCodeType[] ToXmlType(EntitySet<ForeignCitizenCountry> fcc, bool isNationality)
        {
            if (fcc != null)
            {
                return fcc.Where(f => f.IsNationality = isNationality)
                    .OrderBy(f => f.Ordinal)
                    .Select(f => ForeignCitizenCountry.ToXmlType(f))
                    .Where(c => c != null)
                    .ToArray();
            }
            return null;
        }

        public static ForeignCitizenCountry FromXmlType(CountryIdentificationCodeType oio, bool isNationality, int ordinal)
        {
            if (oio != null)
            {
                return new ForeignCitizenCountry()
                {
                    CountryRef = CountryRef.FromXmlType(oio),
                    IsNationality = isNationality,
                    Ordinal = ordinal,
                };
            }
            return null;
        }

        public static ForeignCitizenCountry[] FromXmlType(CountryIdentificationCodeType[] countries, bool isNationality)
        {
            if (countries != null)
            {
                int ordinal = 0;
                return countries
                    .Select(c => ForeignCitizenCountry.FromXmlType(c, isNationality, ordinal++))
                    .Where(c => c != null)
                    .ToArray();
            }
            return new ForeignCitizenCountry[0];
        }


    }


}
