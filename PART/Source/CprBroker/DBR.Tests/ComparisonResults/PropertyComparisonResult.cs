﻿using CprBroker.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace CprBroker.Tests.DBR.ComparisonResults
{
    public class PropertyComparisonResult
    {
        public string PropertyName { get; set; }
        public string SourceName { get; set; }
        public bool IsExcluded { get; set; }
        public bool IsExcluded90 { get; set; }
        public string Remarks { get; set; }

        public override string ToString()
        {
            return string.Format("*** {0}{1}{2}",
                string.Format("Property <{0}>, ", this.PropertyName),
                this.SourceName != null ? string.Format("Column <{0}>, ", this.SourceName) : null,
                string.Format("Excluded <{0}>\r\n", this.IsExcluded)
                );
        }

        public PropertyComparisonResult()
        {
        }

        public PropertyComparisonResult(string name, string comment)
        {
            PropertyName = name;
            SourceName = null;
            IsExcluded = true;
            Remarks = comment;
        }

        public static PropertyComparisonResult FromLinqProperty(PropertyInfo prop, bool isExcluded,bool isExcluded90 )
        {
            return new PropertyComparisonResult()
            {
                PropertyName = prop.Name,
                SourceName = DataLinq.GetColumnName(prop),
                IsExcluded = isExcluded,
                IsExcluded90 = isExcluded90,
                Remarks = null,
            };
        }
    }
}