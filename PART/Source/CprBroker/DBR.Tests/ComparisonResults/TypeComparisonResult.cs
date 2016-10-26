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
        public bool? IgnoreCount { get; set; } = null;

        public List<PropertyComparisonResult> Properties { get; } = new List<PropertyComparisonResult>();

        public List<PropertyComparisonResult> Included { get { return Properties.Where(f => !f.IsExcluded && !f.IsExcluded90).ToList(); } }
        public List<PropertyComparisonResult> Excluded { get { return Properties.Where(f => f.IsExcluded).ToList(); } }
        public List<PropertyComparisonResult> Excluded90 { get { return Properties.Where(f => f.IsExcluded90).ToList(); } }

        public override string ToString()
        {
            var sb = new StringBuilder();

            sb.Append(String.Format("* Type <{0}>, table <{1}>\r\n", ClassName, SourceName));

            if (IgnoreCount.HasValue)
            {
                sb.AppendFormat("** Ignore row count <{0}>\r\n", IgnoreCount);
            }

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
                sb.Append("*** (None)\r\n");
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

            sb.Append("** Non matching for dead people\r\n");
            if (Excluded90.Count > 0)
            {
                foreach (var prop in Excluded90)
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
            var cmpObj = Reflection.CreateInstance(t) as IComparisonType;
            var tableType = cmpObj.TargetType;

            //if (DataLinq.IsTable(tableType))
            {
                var typeMatch = new TypeComparisonResult() { ClassName = tableType.Name, SourceName = cmpObj.SourceName };

                var ignoreCountProp = t.GetProperty("IgnoreCount");
                typeMatch.IgnoreCount = ignoreCountProp == null ?
                    null
                    :
                    (bool?)ignoreCountProp.GetValue(cmpObj);

                var allProperties = cmpObj.DataProperties();
                var excludedProperties = cmpObj.ExcludedPropertiesInformation;
                var excludedProperties90 = cmpObj.ExcludedPropertiesInformation90;

                var fieldMatches =
                    from prop in allProperties
                    join exProp in excludedProperties on prop.Name equals exProp.PropertyName into exProps
                    join exProp90 in excludedProperties90 on prop.Name equals exProp90.PropertyName into exProps90
                    from exPorpName2 in exProps.DefaultIfEmpty()
                    from exPorpName90 in exProps90.DefaultIfEmpty()
                    select PropertyComparisonResult.FromLinqProperty(prop, exPorpName2 != null, exPorpName90 != null);

                typeMatch.Properties.AddRange(fieldMatches);

                return typeMatch;
            }
            //else
            {
                return null;
            }
        }
    }
}