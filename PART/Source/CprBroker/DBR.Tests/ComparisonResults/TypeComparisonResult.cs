﻿using CprBroker.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CprBroker.Tests.DBR.ComparisonResults
{
    public class TypeComparisonResult
    {
        public string ClassName { get; set; }
        public string SourceName { get; set; }
        public string Remarks { get; set; }
        public List<PropertyComparisonResult> Properties { get; } = new List<PropertyComparisonResult>();

        public List<PropertyComparisonResult> Included { get { return Properties.Where(f => f.IsMatch).ToList(); } }
        public List<PropertyComparisonResult> Excluded { get { return Properties.Where(f => !f.IsMatch).ToList(); } }

        public override string ToString()
        {
            var sb = new StringBuilder();

            sb.Append(String.Format("* Type <{0}>, table <{1}>\r\n", ClassName, SourceName));
            sb.Append("** Matching\r\n");

            if (Included.Count > 0)
            {
                foreach (var prop in Included)
                {
                    sb.Append(prop.ToString());
                }
            }
            else
            {
                sb.Append("** (None)\r\n");
            }

            sb.Append("** Non matching\r\n");
            if (Excluded.Count > 0)
            {
                foreach (var prop in Excluded)
                {
                    sb.Append(prop.ToString());
                }
            }
            else
            {
                sb.Append("*** (None)\r\n");
            }
            return sb.ToString();
        }

        public static TypeComparisonResult FromComparisonClass(Type t)
        {
            var dprType = t.BaseType.GetGenericArguments().FirstOrDefault();
            var cmpObj = Reflection.CreateInstance(t);

            if (DataLinq.IsTable(dprType))
            {
                var typeMatch = new TypeComparisonResult() { ClassName = dprType.Name, SourceName = Utilities.DataLinq.GetTableName(dprType) };

                var allProperties = DataLinq.GetColumnProperties(dprType);
                var excludedPropertyNames = t.InvokeMember("ExcludedProperties", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.GetProperty, null, cmpObj, null) as string[];

                var fieldMatches =
                    from prop in allProperties
                    join exPropName in excludedPropertyNames on prop.Name equals exPropName into outer
                    from exPorpName2 in outer.DefaultIfEmpty()
                    select PropertyComparisonResult.FromLinqProperty(prop, exPorpName2 == null);

                typeMatch.Properties.AddRange(fieldMatches);

                return typeMatch;
            }
            else
            {
                return null;
            }
        }
    }
}
