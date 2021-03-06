
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using CprBroker.Schemas.Wrappers;
    
    namespace CprBroker.Providers.CPRDirect
    {
    
    public partial class GeoStartRecordType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 17; }
        }
        #endregion

        #region Properties

        ///  <summary>
        /// Danish: RECORDTYPE
        /// Equals last three digits of the record name
        ///  </summary>
        public decimal RecordType
        {
            get { return this.GetDecimal(1, 3); }
            set { this.SetDecimal(value, 1, 3); }
        }
        ///  <summary>
        /// Danish: OPGAVENR
        ///  </summary>
        public decimal TaskNumber
        {
            get { return this.GetDecimal(4, 6); }
            set { this.SetDecimal(value, 4, 6); }
        }
        ///  <summary>
        /// Danish: PRODDTO
        /// Production date yyyyMMdd
        ///  </summary>
        public DateTime? ProductionDate
        {
            get { return this.GetDateTime(10, 8, "yyyyMMdd"); }
            set { this.SetDateTime(value, 10, 8, "yyyyMMdd"); }
        }

        public Decimal ProductionDateDecimal
        {
            get { return this.GetDecimal(10, 8); }
            set { this.SetDecimal(value, 10, 8); }
        }


        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("RecordType", 1, 3),
                    new Tuple<string, int, int>("TaskNumber", 4, 6),
                    new Tuple<string, int, int>("ProductionDate", 10, 8)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    public partial class StreetType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 111; }
        }
        #endregion

        #region Properties

        ///  <summary>
        /// Danish: KOMKOD
        /// The municipality code
        ///  </summary>
        public decimal MunicipalityCode
        {
            get { return this.GetDecimal(4, 4); }
            set { this.SetDecimal(value, 4, 4); }
        }
        ///  <summary>
        /// Danish: VEJKOD
        /// The street code
        ///  </summary>
        public decimal StreetCode
        {
            get { return this.GetDecimal(8, 4); }
            set { this.SetDecimal(value, 8, 4); }
        }
        ///  <summary>
        /// Danish: VEJADRNVN
        /// The addressing street name
        ///  </summary>
        public string StreetAddressingName
        {
            get { return this.GetString(52, 20); }
            set { this.SetString(value, 52, 20); }
        }

        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("MunicipalityCode", 4, 4),
                    new Tuple<string, int, int>("StreetCode", 8, 4),
                    new Tuple<string, int, int>("StreetAddressingName", 52, 20)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    public partial class ResidenceType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 92; }
        }
        #endregion

        #region Properties

        ///  <summary>
        /// Danish: KOMKOD
        /// The municipality code
        ///  </summary>
        public decimal MunicipalityCode
        {
            get { return this.GetDecimal(4, 4); }
            set { this.SetDecimal(value, 4, 4); }
        }

        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("MunicipalityCode", 4, 4)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    public partial class CityType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 66; }
        }
        #endregion

        #region Properties

        ///  <summary>
        /// Danish: KOMKOD
        /// The municipality code
        ///  </summary>
        public decimal MunicipalityCode
        {
            get { return this.GetDecimal(4, 4); }
            set { this.SetDecimal(value, 4, 4); }
        }
        ///  <summary>
        /// Danish: VEJKOD
        /// The street code
        ///  </summary>
        public decimal StreetCode
        {
            get { return this.GetDecimal(8, 4); }
            set { this.SetDecimal(value, 8, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRFRA
        /// House number from
        ///  </summary>
        public string HouseNumberFrom
        {
            get { return this.GetString(12, 4); }
            set { this.SetString(value, 12, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRTIL
        /// House number to
        ///  </summary>
        public string HouseNumberTo
        {
            get { return this.GetString(16, 4); }
            set { this.SetString(value, 16, 4); }
        }
        ///  <summary>
        /// Danish: LIGEULIGE
        /// Even or odd house number
        ///  </summary>
        public char EvenOrOdd
        {
            get { return this.GetChar(20); }
            set { this.SetChar(value, 20); }
        }
        ///  <summary>
        /// Danish: TIMESTAMP
        /// Date of report
        ///  </summary>
        public decimal Timestamp
        {
            get { return this.GetDecimal(21, 12); }
            set { this.SetDecimal(value, 21, 12); }
        }
        ///  <summary>
        /// Danish: BYNVN
        /// City name
        ///  </summary>
        public string CityName
        {
            get { return this.GetString(33, 34); }
            set { this.SetString(value, 33, 34); }
        }

        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("MunicipalityCode", 4, 4),
                    new Tuple<string, int, int>("StreetCode", 8, 4),
                    new Tuple<string, int, int>("HouseNumberFrom", 12, 4),
                    new Tuple<string, int, int>("HouseNumberTo", 16, 4),
                    new Tuple<string, int, int>("EvenOrOdd", 20, 1),
                    new Tuple<string, int, int>("Timestamp", 21, 12),
                    new Tuple<string, int, int>("CityName", 33, 34)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    public partial class PostDistrictType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 56; }
        }
        #endregion

        #region Properties

        ///  <summary>
        /// Danish: KOMKOD
        /// The municipality code
        ///  </summary>
        public decimal MunicipalityCode
        {
            get { return this.GetDecimal(4, 4); }
            set { this.SetDecimal(value, 4, 4); }
        }
        ///  <summary>
        /// Danish: VEJKOD
        /// The street code
        ///  </summary>
        public decimal StreetCode
        {
            get { return this.GetDecimal(8, 4); }
            set { this.SetDecimal(value, 8, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRFRA
        /// House number from
        ///  </summary>
        public string HouseNumberFrom
        {
            get { return this.GetString(12, 4); }
            set { this.SetString(value, 12, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRTIL
        /// House number to
        ///  </summary>
        public string HouseNumberTo
        {
            get { return this.GetString(16, 4); }
            set { this.SetString(value, 16, 4); }
        }
        ///  <summary>
        /// Danish: LIGEULIGE
        /// Even or odd house number
        ///  </summary>
        public char EvenOrOdd
        {
            get { return this.GetChar(20); }
            set { this.SetChar(value, 20); }
        }
        ///  <summary>
        /// Danish: TIMESTAMP
        /// Date of report
        ///  </summary>
        public decimal Timestamp
        {
            get { return this.GetDecimal(21, 12); }
            set { this.SetDecimal(value, 21, 12); }
        }
        ///  <summary>
        /// Danish: POSTNR
        /// Post number
        ///  </summary>
        public decimal PostNumber
        {
            get { return this.GetDecimal(33, 4); }
            set { this.SetDecimal(value, 33, 4); }
        }
        ///  <summary>
        /// Danish: POSTDISTTXT
        /// Post District text
        ///  </summary>
        public string PostDistrictText
        {
            get { return this.GetString(37, 20); }
            set { this.SetString(value, 37, 20); }
        }

        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("MunicipalityCode", 4, 4),
                    new Tuple<string, int, int>("StreetCode", 8, 4),
                    new Tuple<string, int, int>("HouseNumberFrom", 12, 4),
                    new Tuple<string, int, int>("HouseNumberTo", 16, 4),
                    new Tuple<string, int, int>("EvenOrOdd", 20, 1),
                    new Tuple<string, int, int>("Timestamp", 21, 12),
                    new Tuple<string, int, int>("PostNumber", 33, 4),
                    new Tuple<string, int, int>("PostDistrictText", 37, 20)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    public partial class NoteType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 77; }
        }
        #endregion

        #region Properties

        ///  <summary>
        /// Danish: KOMKOD
        /// The municipality code
        ///  </summary>
        public decimal MunicipalityCode
        {
            get { return this.GetDecimal(4, 4); }
            set { this.SetDecimal(value, 4, 4); }
        }

        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("MunicipalityCode", 4, 4)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    public partial class AreaRestorationDistrictType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 68; }
        }
        #endregion

        #region Properties

        ///  <summary>
        /// Danish: KOMKOD
        /// The municipality code
        ///  </summary>
        public decimal MunicipalityCode
        {
            get { return this.GetDecimal(4, 4); }
            set { this.SetDecimal(value, 4, 4); }
        }
        ///  <summary>
        /// Danish: VEJKOD
        /// The street code
        ///  </summary>
        public decimal StreetCode
        {
            get { return this.GetDecimal(8, 4); }
            set { this.SetDecimal(value, 8, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRFRA
        /// House number from
        ///  </summary>
        public string HouseNumberFrom
        {
            get { return this.GetString(12, 4); }
            set { this.SetString(value, 12, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRTIL
        /// House number to
        ///  </summary>
        public string HouseNumberTo
        {
            get { return this.GetString(16, 4); }
            set { this.SetString(value, 16, 4); }
        }
        ///  <summary>
        /// Danish: LIGEULIGE
        /// Even or Odd house number
        ///  </summary>
        public char EvenOrOdd
        {
            get { return this.GetChar(20); }
            set { this.SetChar(value, 20); }
        }
        ///  <summary>
        /// Danish: TIMESTAMP
        /// Date of report
        ///  </summary>
        public decimal Timestamp
        {
            get { return this.GetDecimal(21, 12); }
            set { this.SetDecimal(value, 21, 12); }
        }
        ///  <summary>
        /// Danish: BYFORNYKOD
        /// Area restoration code
        ///  </summary>
        public string AreaRestorationCode
        {
            get { return this.GetString(33, 6); }
            set { this.SetString(value, 33, 6); }
        }
        ///  <summary>
        /// Danish: DISTTXT
        /// District text
        ///  </summary>
        public string DistrictText
        {
            get { return this.GetString(39, 30); }
            set { this.SetString(value, 39, 30); }
        }

        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("MunicipalityCode", 4, 4),
                    new Tuple<string, int, int>("StreetCode", 8, 4),
                    new Tuple<string, int, int>("HouseNumberFrom", 12, 4),
                    new Tuple<string, int, int>("HouseNumberTo", 16, 4),
                    new Tuple<string, int, int>("EvenOrOdd", 20, 1),
                    new Tuple<string, int, int>("Timestamp", 21, 12),
                    new Tuple<string, int, int>("AreaRestorationCode", 33, 6),
                    new Tuple<string, int, int>("DistrictText", 39, 30)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    public partial class DiverseDistrictType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 68; }
        }
        #endregion

        #region Properties

        ///  <summary>
        /// Danish: KOMKOD
        /// The municipality code
        ///  </summary>
        public decimal MunicipalityCode
        {
            get { return this.GetDecimal(4, 4); }
            set { this.SetDecimal(value, 4, 4); }
        }
        ///  <summary>
        /// Danish: VEJKOD
        /// The street code
        ///  </summary>
        public decimal StreetCode
        {
            get { return this.GetDecimal(8, 4); }
            set { this.SetDecimal(value, 8, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRFRA
        /// House number from
        ///  </summary>
        public string HouseNumberFrom
        {
            get { return this.GetString(12, 4); }
            set { this.SetString(value, 12, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRTIL
        /// House number to
        ///  </summary>
        public string HouseNumberTo
        {
            get { return this.GetString(16, 4); }
            set { this.SetString(value, 16, 4); }
        }
        ///  <summary>
        /// Danish: LIGEULIGE
        /// Even or Odd house number
        ///  </summary>
        public char EvenOrOdd
        {
            get { return this.GetChar(20); }
            set { this.SetChar(value, 20); }
        }
        ///  <summary>
        /// Danish: TIMESTAMP
        /// Date of report
        ///  </summary>
        public decimal Timestamp
        {
            get { return this.GetDecimal(21, 12); }
            set { this.SetDecimal(value, 21, 12); }
        }
        ///  <summary>
        /// Danish: DISTTYP
        /// District type
        ///  </summary>
        public decimal DistrictType
        {
            get { return this.GetDecimal(33, 2); }
            set { this.SetDecimal(value, 33, 2); }
        }
        ///  <summary>
        /// Danish: DIVDISTKOD
        /// Diverse district code
        ///  </summary>
        public string DivDistrictCode
        {
            get { return this.GetString(35, 4); }
            set { this.SetString(value, 35, 4); }
        }
        ///  <summary>
        /// Danish: DISTTXT
        /// District text
        ///  </summary>
        public string DistrictText
        {
            get { return this.GetString(39, 30); }
            set { this.SetString(value, 39, 30); }
        }

        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("MunicipalityCode", 4, 4),
                    new Tuple<string, int, int>("StreetCode", 8, 4),
                    new Tuple<string, int, int>("HouseNumberFrom", 12, 4),
                    new Tuple<string, int, int>("HouseNumberTo", 16, 4),
                    new Tuple<string, int, int>("EvenOrOdd", 20, 1),
                    new Tuple<string, int, int>("Timestamp", 21, 12),
                    new Tuple<string, int, int>("DistrictType", 33, 2),
                    new Tuple<string, int, int>("DivDistrictCode", 35, 4),
                    new Tuple<string, int, int>("DistrictText", 39, 30)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    public partial class EvacuationDistrictType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 63; }
        }
        #endregion

        #region Properties

        ///  <summary>
        /// Danish: KOMKOD
        /// The municipality code
        ///  </summary>
        public decimal MunicipalityCode
        {
            get { return this.GetDecimal(4, 4); }
            set { this.SetDecimal(value, 4, 4); }
        }
        ///  <summary>
        /// Danish: VEJKOD
        /// The street code
        ///  </summary>
        public decimal StreetCode
        {
            get { return this.GetDecimal(8, 4); }
            set { this.SetDecimal(value, 8, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRFRA
        /// House number from
        ///  </summary>
        public string HouseNumberFrom
        {
            get { return this.GetString(12, 4); }
            set { this.SetString(value, 12, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRTIL
        /// House number to
        ///  </summary>
        public string HouseNumberTo
        {
            get { return this.GetString(16, 4); }
            set { this.SetString(value, 16, 4); }
        }
        ///  <summary>
        /// Danish: LIGEULIGE
        /// Even or Odd house number
        ///  </summary>
        public char EvenOrOdd
        {
            get { return this.GetChar(20); }
            set { this.SetChar(value, 20); }
        }
        ///  <summary>
        /// Danish: TIMESTAMP
        /// Date of report
        ///  </summary>
        public decimal Timestamp
        {
            get { return this.GetDecimal(21, 12); }
            set { this.SetDecimal(value, 21, 12); }
        }
        ///  <summary>
        /// Danish: EVAKUERKOD
        /// Evacuation code
        ///  </summary>
        public decimal EvacuationCode
        {
            get { return this.GetDecimal(33, 1); }
            set { this.SetDecimal(value, 33, 1); }
        }
        ///  <summary>
        /// Danish: DISTTXT
        /// District text
        ///  </summary>
        public string DistrictText
        {
            get { return this.GetString(34, 30); }
            set { this.SetString(value, 34, 30); }
        }

        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("MunicipalityCode", 4, 4),
                    new Tuple<string, int, int>("StreetCode", 8, 4),
                    new Tuple<string, int, int>("HouseNumberFrom", 12, 4),
                    new Tuple<string, int, int>("HouseNumberTo", 16, 4),
                    new Tuple<string, int, int>("EvenOrOdd", 20, 1),
                    new Tuple<string, int, int>("Timestamp", 21, 12),
                    new Tuple<string, int, int>("EvacuationCode", 33, 1),
                    new Tuple<string, int, int>("DistrictText", 34, 30)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    public partial class ChurchDistrictType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 64; }
        }
        #endregion

        #region Properties

        ///  <summary>
        /// Danish: KOMKOD
        /// The municipality code
        ///  </summary>
        public decimal MunicipalityCode
        {
            get { return this.GetDecimal(4, 4); }
            set { this.SetDecimal(value, 4, 4); }
        }
        ///  <summary>
        /// Danish: VEJKOD
        /// The street code
        ///  </summary>
        public decimal StreetCode
        {
            get { return this.GetDecimal(8, 4); }
            set { this.SetDecimal(value, 8, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRFRA
        /// House number from
        ///  </summary>
        public string HouseNumberFrom
        {
            get { return this.GetString(12, 4); }
            set { this.SetString(value, 12, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRTIL
        /// House number to
        ///  </summary>
        public string HouseNumberTo
        {
            get { return this.GetString(16, 4); }
            set { this.SetString(value, 16, 4); }
        }
        ///  <summary>
        /// Danish: LIGEULIGE
        /// Even or Odd house number
        ///  </summary>
        public char EvenOrOdd
        {
            get { return this.GetChar(20); }
            set { this.SetChar(value, 20); }
        }
        ///  <summary>
        /// Danish: TIMESTAMP
        /// Date of report
        ///  </summary>
        public decimal Timestamp
        {
            get { return this.GetDecimal(21, 12); }
            set { this.SetDecimal(value, 21, 12); }
        }
        ///  <summary>
        /// Danish: KIRKEKOD
        /// Church district code
        ///  </summary>
        public decimal ChurchDistrictCode
        {
            get { return this.GetDecimal(33, 2); }
            set { this.SetDecimal(value, 33, 2); }
        }
        ///  <summary>
        /// Danish: DISTTXT
        /// District text
        ///  </summary>
        public string DistrictText
        {
            get { return this.GetString(35, 30); }
            set { this.SetString(value, 35, 30); }
        }

        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("MunicipalityCode", 4, 4),
                    new Tuple<string, int, int>("StreetCode", 8, 4),
                    new Tuple<string, int, int>("HouseNumberFrom", 12, 4),
                    new Tuple<string, int, int>("HouseNumberTo", 16, 4),
                    new Tuple<string, int, int>("EvenOrOdd", 20, 1),
                    new Tuple<string, int, int>("Timestamp", 21, 12),
                    new Tuple<string, int, int>("ChurchDistrictCode", 33, 2),
                    new Tuple<string, int, int>("DistrictText", 35, 30)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    public partial class SchoolDistrictType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 64; }
        }
        #endregion

        #region Properties

        ///  <summary>
        /// Danish: KOMKOD
        /// The municipality code
        ///  </summary>
        public decimal MunicipalityCode
        {
            get { return this.GetDecimal(4, 4); }
            set { this.SetDecimal(value, 4, 4); }
        }
        ///  <summary>
        /// Danish: VEJKOD
        /// The street code
        ///  </summary>
        public decimal StreetCode
        {
            get { return this.GetDecimal(8, 4); }
            set { this.SetDecimal(value, 8, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRFRA
        /// House number from
        ///  </summary>
        public string HouseNumberFrom
        {
            get { return this.GetString(12, 4); }
            set { this.SetString(value, 12, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRTIL
        /// House number to
        ///  </summary>
        public string HouseNumberTo
        {
            get { return this.GetString(16, 4); }
            set { this.SetString(value, 16, 4); }
        }
        ///  <summary>
        /// Danish: LIGEULIGE
        /// Even or Odd house number
        ///  </summary>
        public char EvenOrOdd
        {
            get { return this.GetChar(20); }
            set { this.SetChar(value, 20); }
        }
        ///  <summary>
        /// Danish: TIMESTAMP
        /// Date of report
        ///  </summary>
        public decimal Timestamp
        {
            get { return this.GetDecimal(21, 12); }
            set { this.SetDecimal(value, 21, 12); }
        }
        ///  <summary>
        /// Danish: SKOLEKOD
        /// School code
        ///  </summary>
        public decimal SchoolCode
        {
            get { return this.GetDecimal(33, 2); }
            set { this.SetDecimal(value, 33, 2); }
        }
        ///  <summary>
        /// Danish: DISTTXT
        /// District text
        ///  </summary>
        public string DistrictText
        {
            get { return this.GetString(35, 30); }
            set { this.SetString(value, 35, 30); }
        }

        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("MunicipalityCode", 4, 4),
                    new Tuple<string, int, int>("StreetCode", 8, 4),
                    new Tuple<string, int, int>("HouseNumberFrom", 12, 4),
                    new Tuple<string, int, int>("HouseNumberTo", 16, 4),
                    new Tuple<string, int, int>("EvenOrOdd", 20, 1),
                    new Tuple<string, int, int>("Timestamp", 21, 12),
                    new Tuple<string, int, int>("SchoolCode", 33, 2),
                    new Tuple<string, int, int>("DistrictText", 35, 30)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    public partial class PopulationDistrictType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 66; }
        }
        #endregion

        #region Properties

        ///  <summary>
        /// Danish: KOMKOD
        /// The municipality code
        ///  </summary>
        public decimal MunicipalityCode
        {
            get { return this.GetDecimal(4, 4); }
            set { this.SetDecimal(value, 4, 4); }
        }
        ///  <summary>
        /// Danish: VEJKOD
        /// The street code
        ///  </summary>
        public decimal StreetCode
        {
            get { return this.GetDecimal(8, 4); }
            set { this.SetDecimal(value, 8, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRFRA
        /// House number from
        ///  </summary>
        public string HouseNumberFrom
        {
            get { return this.GetString(12, 4); }
            set { this.SetString(value, 12, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRTIL
        /// House number to
        ///  </summary>
        public string HouseNumberTo
        {
            get { return this.GetString(16, 4); }
            set { this.SetString(value, 16, 4); }
        }
        ///  <summary>
        /// Danish: LIGEULIGE
        /// Even or Odd house number
        ///  </summary>
        public char EvenOrOdd
        {
            get { return this.GetChar(20); }
            set { this.SetChar(value, 20); }
        }
        ///  <summary>
        /// Danish: TIMESTAMP
        /// Date of report
        ///  </summary>
        public decimal Timestamp
        {
            get { return this.GetDecimal(21, 12); }
            set { this.SetDecimal(value, 21, 12); }
        }
        ///  <summary>
        /// Danish: BEFOLKKOD
        /// Population district code
        ///  </summary>
        public string PopulationDistrictCode
        {
            get { return this.GetString(33, 4); }
            set { this.SetString(value, 33, 4); }
        }
        ///  <summary>
        /// Danish: DISTTXT
        /// District text
        ///  </summary>
        public string DistrictText
        {
            get { return this.GetString(37, 30); }
            set { this.SetString(value, 37, 30); }
        }

        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("MunicipalityCode", 4, 4),
                    new Tuple<string, int, int>("StreetCode", 8, 4),
                    new Tuple<string, int, int>("HouseNumberFrom", 12, 4),
                    new Tuple<string, int, int>("HouseNumberTo", 16, 4),
                    new Tuple<string, int, int>("EvenOrOdd", 20, 1),
                    new Tuple<string, int, int>("Timestamp", 21, 12),
                    new Tuple<string, int, int>("PopulationDistrictCode", 33, 4),
                    new Tuple<string, int, int>("DistrictText", 37, 30)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    public partial class SocialDistrictType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 64; }
        }
        #endregion

        #region Properties

        ///  <summary>
        /// Danish: KOMKOD
        /// The municipality code
        ///  </summary>
        public decimal MunicipalityCode
        {
            get { return this.GetDecimal(4, 4); }
            set { this.SetDecimal(value, 4, 4); }
        }
        ///  <summary>
        /// Danish: VEJKOD
        /// The street code
        ///  </summary>
        public decimal StreetCode
        {
            get { return this.GetDecimal(8, 4); }
            set { this.SetDecimal(value, 8, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRFRA
        /// House number from
        ///  </summary>
        public string HouseNumberFrom
        {
            get { return this.GetString(12, 4); }
            set { this.SetString(value, 12, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRTIL
        /// House number to
        ///  </summary>
        public string HouseNumberTo
        {
            get { return this.GetString(16, 4); }
            set { this.SetString(value, 16, 4); }
        }
        ///  <summary>
        /// Danish: LIGEULIGE
        /// Even or Odd house number
        ///  </summary>
        public char EvenOrOdd
        {
            get { return this.GetChar(20); }
            set { this.SetChar(value, 20); }
        }
        ///  <summary>
        /// Danish: TIMESTAMP
        /// Date of report
        ///  </summary>
        public decimal Timestamp
        {
            get { return this.GetDecimal(21, 12); }
            set { this.SetDecimal(value, 21, 12); }
        }
        ///  <summary>
        /// Danish: SOCIALKOD
        /// Social code
        ///  </summary>
        public decimal SocialCode
        {
            get { return this.GetDecimal(33, 2); }
            set { this.SetDecimal(value, 33, 2); }
        }
        ///  <summary>
        /// Danish: DISTTXT
        /// District text
        ///  </summary>
        public string DistrictText
        {
            get { return this.GetString(35, 30); }
            set { this.SetString(value, 35, 30); }
        }

        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("MunicipalityCode", 4, 4),
                    new Tuple<string, int, int>("StreetCode", 8, 4),
                    new Tuple<string, int, int>("HouseNumberFrom", 12, 4),
                    new Tuple<string, int, int>("HouseNumberTo", 16, 4),
                    new Tuple<string, int, int>("EvenOrOdd", 20, 1),
                    new Tuple<string, int, int>("Timestamp", 21, 12),
                    new Tuple<string, int, int>("SocialCode", 33, 2),
                    new Tuple<string, int, int>("DistrictText", 35, 30)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    public partial class ChurchAdministrationDistrictType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 56; }
        }
        #endregion

        #region Properties

        ///  <summary>
        /// Danish: KOMKOD
        /// The municipality code
        ///  </summary>
        public decimal MunicipalityCode
        {
            get { return this.GetDecimal(4, 4); }
            set { this.SetDecimal(value, 4, 4); }
        }
        ///  <summary>
        /// Danish: VEJKOD
        /// The street code
        ///  </summary>
        public decimal StreetCode
        {
            get { return this.GetDecimal(8, 4); }
            set { this.SetDecimal(value, 8, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRFRA
        /// House number from
        ///  </summary>
        public string HouseNumberFrom
        {
            get { return this.GetString(12, 4); }
            set { this.SetString(value, 12, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRTIL
        /// House number to
        ///  </summary>
        public string HouseNumberTo
        {
            get { return this.GetString(16, 4); }
            set { this.SetString(value, 16, 4); }
        }
        ///  <summary>
        /// Danish: LIGEULIGE
        /// Even or Odd house number
        ///  </summary>
        public char EvenOrOdd
        {
            get { return this.GetChar(20); }
            set { this.SetChar(value, 20); }
        }
        ///  <summary>
        /// Danish: TIMESTAMP
        /// Date of report
        ///  </summary>
        public decimal Timestamp
        {
            get { return this.GetDecimal(21, 12); }
            set { this.SetDecimal(value, 21, 12); }
        }
        ///  <summary>
        /// Danish: MYNKOD-CTSOGNEDIST
        /// Authority and church code
        ///  </summary>
        public decimal AuthorityAndChurchCode
        {
            get { return this.GetDecimal(33, 4); }
            set { this.SetDecimal(value, 33, 4); }
        }
        ///  <summary>
        /// Danish: MYNNVN
        /// Authority name
        ///  </summary>
        public string AuthorityName
        {
            get { return this.GetString(37, 20); }
            set { this.SetString(value, 37, 20); }
        }

        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("MunicipalityCode", 4, 4),
                    new Tuple<string, int, int>("StreetCode", 8, 4),
                    new Tuple<string, int, int>("HouseNumberFrom", 12, 4),
                    new Tuple<string, int, int>("HouseNumberTo", 16, 4),
                    new Tuple<string, int, int>("EvenOrOdd", 20, 1),
                    new Tuple<string, int, int>("Timestamp", 21, 12),
                    new Tuple<string, int, int>("AuthorityAndChurchCode", 33, 4),
                    new Tuple<string, int, int>("AuthorityName", 37, 20)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    public partial class ElectionDistrictType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 64; }
        }
        #endregion

        #region Properties

        ///  <summary>
        /// Danish: KOMKOD
        /// The municipality code
        ///  </summary>
        public decimal MunicipalityCode
        {
            get { return this.GetDecimal(4, 4); }
            set { this.SetDecimal(value, 4, 4); }
        }
        ///  <summary>
        /// Danish: VEJKOD
        /// The street code
        ///  </summary>
        public decimal StreetCode
        {
            get { return this.GetDecimal(8, 4); }
            set { this.SetDecimal(value, 8, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRFRA
        /// House number from
        ///  </summary>
        public string HouseNumberFrom
        {
            get { return this.GetString(12, 4); }
            set { this.SetString(value, 12, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRTIL
        /// House number to
        ///  </summary>
        public string HouseNumberTo
        {
            get { return this.GetString(16, 4); }
            set { this.SetString(value, 16, 4); }
        }
        ///  <summary>
        /// Danish: LIGEULIGE
        /// Even or Odd house number
        ///  </summary>
        public char EvenOrOdd
        {
            get { return this.GetChar(20); }
            set { this.SetChar(value, 20); }
        }
        ///  <summary>
        /// Danish: TIMESTAMP
        /// Date of report
        ///  </summary>
        public decimal Timestamp
        {
            get { return this.GetDecimal(21, 12); }
            set { this.SetDecimal(value, 21, 12); }
        }
        ///  <summary>
        /// Danish: VALGKOD
        /// Election code
        ///  </summary>
        public decimal ElectionCode
        {
            get { return this.GetDecimal(33, 2); }
            set { this.SetDecimal(value, 33, 2); }
        }
        ///  <summary>
        /// Danish: DISTTXT
        /// District text
        ///  </summary>
        public string DistrictText
        {
            get { return this.GetString(35, 30); }
            set { this.SetString(value, 35, 30); }
        }

        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("MunicipalityCode", 4, 4),
                    new Tuple<string, int, int>("StreetCode", 8, 4),
                    new Tuple<string, int, int>("HouseNumberFrom", 12, 4),
                    new Tuple<string, int, int>("HouseNumberTo", 16, 4),
                    new Tuple<string, int, int>("EvenOrOdd", 20, 1),
                    new Tuple<string, int, int>("Timestamp", 21, 12),
                    new Tuple<string, int, int>("ElectionCode", 33, 2),
                    new Tuple<string, int, int>("DistrictText", 35, 30)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    public partial class HeatingDistrictType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 66; }
        }
        #endregion

        #region Properties

        ///  <summary>
        /// Danish: KOMKOD
        /// The municipality code
        ///  </summary>
        public decimal MunicipalityCode
        {
            get { return this.GetDecimal(4, 4); }
            set { this.SetDecimal(value, 4, 4); }
        }
        ///  <summary>
        /// Danish: VEJKOD
        /// The street code
        ///  </summary>
        public decimal StreetCode
        {
            get { return this.GetDecimal(8, 4); }
            set { this.SetDecimal(value, 8, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRFRA
        /// House number from
        ///  </summary>
        public string HouseNumberFrom
        {
            get { return this.GetString(12, 4); }
            set { this.SetString(value, 12, 4); }
        }
        ///  <summary>
        /// Danish: HUSNRTIL
        /// House number to
        ///  </summary>
        public string HouseNumberTo
        {
            get { return this.GetString(16, 4); }
            set { this.SetString(value, 16, 4); }
        }
        ///  <summary>
        /// Danish: LIGEULIGE
        /// Even or Odd house number
        ///  </summary>
        public char EvenOrOdd
        {
            get { return this.GetChar(20); }
            set { this.SetChar(value, 20); }
        }
        ///  <summary>
        /// Danish: TIMESTAMP
        /// Date of report
        ///  </summary>
        public decimal Timestamp
        {
            get { return this.GetDecimal(21, 12); }
            set { this.SetDecimal(value, 21, 12); }
        }
        ///  <summary>
        /// Danish: VARMEKOD
        /// Heating district code
        ///  </summary>
        public decimal HeatingDistrictCode
        {
            get { return this.GetDecimal(33, 2); }
            set { this.SetDecimal(value, 33, 2); }
        }
        ///  <summary>
        /// Danish: DISTTXT
        /// District text
        ///  </summary>
        public string DistrictText
        {
            get { return this.GetString(35, 30); }
            set { this.SetString(value, 35, 30); }
        }

        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("MunicipalityCode", 4, 4),
                    new Tuple<string, int, int>("StreetCode", 8, 4),
                    new Tuple<string, int, int>("HouseNumberFrom", 12, 4),
                    new Tuple<string, int, int>("HouseNumberTo", 16, 4),
                    new Tuple<string, int, int>("EvenOrOdd", 20, 1),
                    new Tuple<string, int, int>("Timestamp", 21, 12),
                    new Tuple<string, int, int>("HeatingDistrictCode", 33, 2),
                    new Tuple<string, int, int>("DistrictText", 35, 30)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    public partial class GeoEndRecordType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 11; }
        }
        #endregion

        #region Properties

        ///  <summary>
        /// Danish: RECORDTYPE
        /// Equals last three digits of the record name
        ///  </summary>
        public decimal RecordType
        {
            get { return this.GetDecimal(1, 3); }
            set { this.SetDecimal(value, 1, 3); }
        }
        ///  <summary>
        /// Danish: TAELLER
        /// Counter
        ///  </summary>
        public string Counter
        {
            get { return this.GetString(4, 8); }
            set { this.SetString(value, 4, 8); }
        }

        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("RecordType", 1, 3),
                    new Tuple<string, int, int>("Counter", 4, 8)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    public partial class PostStartRecordType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 17; }
        }
        #endregion

        #region Properties

        ///  <summary>
        /// Danish: RECORDTYPE
        /// Equals last three digits of the record name
        ///  </summary>
        public decimal RecordType
        {
            get { return this.GetDecimal(1, 3); }
            set { this.SetDecimal(value, 1, 3); }
        }
        ///  <summary>
        /// Danish: OPGAVENR
        ///  </summary>
        public decimal TaskNumber
        {
            get { return this.GetDecimal(4, 6); }
            set { this.SetDecimal(value, 4, 6); }
        }
        ///  <summary>
        /// Danish: PRODDTO
        /// Production date yyyyMMdd
        ///  </summary>
        public DateTime? ProductionDate
        {
            get { return this.GetDateTime(10, 8, "yyyyMMdd"); }
            set { this.SetDateTime(value, 10, 8, "yyyyMMdd"); }
        }

        public Decimal ProductionDateDecimal
        {
            get { return this.GetDecimal(10, 8); }
            set { this.SetDecimal(value, 10, 8); }
        }


        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("RecordType", 1, 3),
                    new Tuple<string, int, int>("TaskNumber", 4, 6),
                    new Tuple<string, int, int>("ProductionDate", 10, 8)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    public partial class PostNumberType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 84; }
        }
        #endregion

        #region Properties

        ///  <summary>
        /// Danish: POSTNR
        /// The post code
        ///  </summary>
        public decimal PostCode
        {
            get { return this.GetDecimal(61, 4); }
            set { this.SetDecimal(value, 61, 4); }
        }
        ///  <summary>
        /// Danish: POSTDISTTXT
        /// The post text
        ///  </summary>
        public string PostText
        {
            get { return this.GetString(65, 20); }
            set { this.SetString(value, 65, 20); }
        }

        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("PostCode", 61, 4),
                    new Tuple<string, int, int>("PostText", 65, 20)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    public partial class PostEndRecordType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 11; }
        }
        #endregion

        #region Properties

        ///  <summary>
        /// Danish: RECORDTYPE
        /// Equals last three digits of the record name
        ///  </summary>
        public decimal RecordType
        {
            get { return this.GetDecimal(1, 3); }
            set { this.SetDecimal(value, 1, 3); }
        }
        ///  <summary>
        /// Danish: TAELLER
        /// Counter
        ///  </summary>
        public string Counter
        {
            get { return this.GetString(4, 8); }
            set { this.SetString(value, 4, 8); }
        }

        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("RecordType", 1, 3),
                    new Tuple<string, int, int>("Counter", 4, 8)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    }
  