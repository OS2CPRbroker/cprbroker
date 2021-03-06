
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using CprBroker.Schemas.Wrappers;
    
    namespace CprBroker.Providers.CPRDirect
    {
    
    public partial class AuthorityType: Wrapper
    {
        #region Common

        public override int Length
        {
            get { return 420; }
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
        /// Danish: MYNKOD
        /// Authority code
        ///  </summary>
        public string AuthorityCode
        {
            get { return this.GetString(4, 4); }
            set { this.SetString(value, 4, 4); }
        }
        ///  <summary>
        /// Danish: MYNTYP
        /// Authority type
        ///  </summary>
        public string AuthorityTypeCode
        {
            get { return this.GetString(8, 2); }
            set { this.SetString(value, 8, 2); }
        }
        ///  <summary>
        /// Danish: MYNGRP
        /// Authority group
        ///  </summary>
        public char AuthorityGroupCode
        {
            get { return this.GetChar(10); }
            set { this.SetChar(value, 10); }
        }
        ///  <summary>
        /// Danish: TIMESTAMP
        /// Update time ÅÅÅÅMMDDTTMM
        ///  </summary>
        public DateTime? UpdateTime
        {
            get { return this.GetDateTime(11, 12, "yyyyMMddHHmm"); }
            set { this.SetDateTime(value, 11, 12, "yyyyMMddHHmm"); }
        }

        public Decimal UpdateTimeDecimal
        {
            get { return this.GetDecimal(11, 12); }
            set { this.SetDecimal(value, 11, 12); }
        }

        ///  <summary>
        /// Danish: MYNTLF
        /// Authority phone
        ///  </summary>
        public string AuthorityPhone
        {
            get { return this.GetString(23, 8); }
            set { this.SetString(value, 23, 8); }
        }
        ///  <summary>
        /// Danish: HAENSTART
        /// Authority start date ÅÅÅÅMMDDTTMM
        ///  </summary>
        public DateTime? AuthorityStartDate
        {
            get { return this.GetDateTime(31, 12, "yyyyMMddHHmm"); }
            set { this.SetDateTime(value, 31, 12, "yyyyMMddHHmm"); }
        }

        public Decimal AuthorityStartDateDecimal
        {
            get { return this.GetDecimal(31, 12); }
            set { this.SetDecimal(value, 31, 12); }
        }

        ///  <summary>
        /// Danish: HAENSLUT-CTMYN
        /// Authority end date ÅÅÅÅMMDDTTMM
        ///  </summary>
        public DateTime? AuthorityEndDate
        {
            get { return this.GetDateTime(43, 12, "yyyyMMddHHmm"); }
            set { this.SetDateTime(value, 43, 12, "yyyyMMddHHmm"); }
        }

        public Decimal AuthorityEndDateDecimal
        {
            get { return this.GetDecimal(43, 12); }
            set { this.SetDecimal(value, 43, 12); }
        }

        ///  <summary>
        /// Danish: MYNNVN
        /// Authority name
        ///  </summary>
        public string AuthorityName
        {
            get { return this.GetString(55, 20); }
            set { this.SetString(value, 55, 20); }
        }
        ///  <summary>
        /// Danish: MYNADRSAT
        /// Authority addressee
        ///  </summary>
        public string Adressee
        {
            get { return this.GetString(75, 34); }
            set { this.SetString(value, 75, 34); }
        }
        ///  <summary>
        /// Danish: ADR1
        /// Address line 1
        ///  </summary>
        public string Address1
        {
            get { return this.GetString(109, 34); }
            set { this.SetString(value, 109, 34); }
        }
        ///  <summary>
        /// Danish: ADR2
        /// Address line 2
        ///  </summary>
        public string Address2
        {
            get { return this.GetString(143, 34); }
            set { this.SetString(value, 143, 34); }
        }
        ///  <summary>
        /// Danish: ADR3
        /// Address line 3
        ///  </summary>
        public string Address3
        {
            get { return this.GetString(177, 34); }
            set { this.SetString(value, 177, 34); }
        }
        ///  <summary>
        /// Danish: ADR4
        /// Address line 4
        ///  </summary>
        public string Address4
        {
            get { return this.GetString(211, 34); }
            set { this.SetString(value, 211, 34); }
        }
        ///  <summary>
        /// Danish: TELEFAXNR
        /// Telefax
        ///  </summary>
        public string Telefax
        {
            get { return this.GetString(245, 8); }
            set { this.SetString(value, 245, 8); }
        }
        ///  <summary>
        /// Danish: MYNNVN_XL
        /// Full name
        ///  </summary>
        public string FullName
        {
            get { return this.GetString(253, 60); }
            set { this.SetString(value, 253, 60); }
        }
        ///  <summary>
        /// Danish: E_MAIL
        /// Email
        ///  </summary>
        public string Email
        {
            get { return this.GetString(313, 100); }
            set { this.SetString(value, 313, 100); }
        }
        ///  <summary>
        /// Danish: LAND_KOD_ALFA2
        /// Alpha2 country code
        ///  </summary>
        public string Alpha2CountryCode
        {
            get { return this.GetString(413, 2); }
            set { this.SetString(value, 413, 2); }
        }
        ///  <summary>
        /// Danish: LAND_KOD_ALFA3
        /// Alpha3 country code
        ///  </summary>
        public string Alpha3CountryCode
        {
            get { return this.GetString(415, 3); }
            set { this.SetString(value, 415, 3); }
        }
        ///  <summary>
        /// Danish: LAND_KOD_NUM3
        /// Numeric country code
        ///  </summary>
        public string NumericCountryCode
        {
            get { return this.GetString(418, 3); }
            set { this.SetString(value, 418, 3); }
        }

        #endregion
        public override Tuple<string, int, int>[] DefaultPropertyDefinitions
        {
            get 
            {
                var ret = base.DefaultPropertyDefinitions.ToList();
                ret.AddRange(new Tuple<string, int, int>[]{
                    new Tuple<string, int, int>("RecordType", 1, 3),
                    new Tuple<string, int, int>("AuthorityCode", 4, 4),
                    new Tuple<string, int, int>("AuthorityTypeCode", 8, 2),
                    new Tuple<string, int, int>("AuthorityGroupCode", 10, 1),
                    new Tuple<string, int, int>("UpdateTime", 11, 12),
                    new Tuple<string, int, int>("AuthorityPhone", 23, 8),
                    new Tuple<string, int, int>("AuthorityStartDate", 31, 12),
                    new Tuple<string, int, int>("AuthorityEndDate", 43, 12),
                    new Tuple<string, int, int>("AuthorityName", 55, 20),
                    new Tuple<string, int, int>("Adressee", 75, 34),
                    new Tuple<string, int, int>("Address1", 109, 34),
                    new Tuple<string, int, int>("Address2", 143, 34),
                    new Tuple<string, int, int>("Address3", 177, 34),
                    new Tuple<string, int, int>("Address4", 211, 34),
                    new Tuple<string, int, int>("Telefax", 245, 8),
                    new Tuple<string, int, int>("FullName", 253, 60),
                    new Tuple<string, int, int>("Email", 313, 100),
                    new Tuple<string, int, int>("Alpha2CountryCode", 413, 2),
                    new Tuple<string, int, int>("Alpha3CountryCode", 415, 3),
                    new Tuple<string, int, int>("NumericCountryCode", 418, 3)
                });
                return ret.OrderBy(pd => pd.Item2).ToArray();
            }
        }

    }

  
    }
  