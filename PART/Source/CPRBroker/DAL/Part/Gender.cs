﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CprBroker.DAL.Part
{
    public partial class Gender
    {
        private static List<KeyValuePair<string, Schemas.Part.Enums.Gender>> _PartValues = new List<KeyValuePair<string, Schemas.Part.Enums.Gender>>();

        private static void LoadPartValues()
        {
            lock (_PartValues)
            {
                if (_PartValues.Count == 0)
                {
                    _PartValues.AddRange(CprBroker.Schemas.Util.Enums.GetEnumValues<Schemas.Part.Enums.Gender>());
                }
            }
        }

        public static int GetPartCode(Schemas.Part.Enums.Gender personGenderCode)
        {
            LoadPartValues();
            var code = (from kvp in _PartValues where kvp.Value == personGenderCode select kvp.Key).FirstOrDefault();
            return Convert.ToInt32(code);
        }

        public static Schemas.Part.Enums.Gender GetPartGender(int code)
        {
            LoadPartValues();
            string sCode = code.ToString();
            return (from kvp in _PartValues where kvp.Key.Equals(sCode) select kvp.Value).FirstOrDefault();
        }
    }
}
