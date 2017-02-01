﻿#pragma warning disable 1591
//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace CprBroker.Providers.DPR
{
	using System.Data.Linq;
	using System.Data.Linq.Mapping;
	using System.Data;
	using System.Collections.Generic;
	using System.Reflection;
	using System.Linq;
	using System.Linq.Expressions;
	using System.ComponentModel;
	using System;
	
	
	[global::System.Data.Linq.Mapping.DatabaseAttribute(Name="DBR_3525425")]
	public partial class AdminDataContext : System.Data.Linq.DataContext
	{
		
		private static System.Data.Linq.Mapping.MappingSource mappingSource = new AttributeMappingSource();
		
    #region Extensibility Method Definitions
    partial void OnCreated();
    partial void InsertUpdate(Update instance);
    partial void UpdateUpdate(Update instance);
    partial void DeleteUpdate(Update instance);
    #endregion
		
		public AdminDataContext(string connection) : 
				base(connection, mappingSource)
		{
			OnCreated();
		}
		
		public AdminDataContext(System.Data.IDbConnection connection) : 
				base(connection, mappingSource)
		{
			OnCreated();
		}
		
		public AdminDataContext(string connection, System.Data.Linq.Mapping.MappingSource mappingSource) : 
				base(connection, mappingSource)
		{
			OnCreated();
		}
		
		public AdminDataContext(System.Data.IDbConnection connection, System.Data.Linq.Mapping.MappingSource mappingSource) : 
				base(connection, mappingSource)
		{
			OnCreated();
		}
		
		public System.Data.Linq.Table<Update> Updates
		{
			get
			{
				return this.GetTable<Update>();
			}
		}
	}
	
	[global::System.Data.Linq.Mapping.TableAttribute(Name="dbo.DTAJOUR")]
	public partial class Update : INotifyPropertyChanging, INotifyPropertyChanged
	{
		
		private static PropertyChangingEventArgs emptyChangingEventArgs = new PropertyChangingEventArgs(String.Empty);
		
		private decimal _DPRAJDTO;
		
		private decimal _ANTAL;
		
		private decimal _KOMKOD;
		
		private string _KOMNVN;
		
		private string _SOEGMRK;
		
		private decimal _HISMDR;
		
		private string _CICSID;
		
		private System.Nullable<decimal> _KUNDENR;
		
		private string _LU62MRK;
		
		private string _LUNAVN;
		
		private System.Nullable<decimal> _LEVAJDTO;
		
		private string _DB_VERSION;
		
    #region Extensibility Method Definitions
    partial void OnLoaded();
    partial void OnValidate(System.Data.Linq.ChangeAction action);
    partial void OnCreated();
    partial void OnDPRAJDTOChanging(decimal value);
    partial void OnDPRAJDTOChanged();
    partial void OnANTALChanging(decimal value);
    partial void OnANTALChanged();
    partial void OnKOMKODChanging(decimal value);
    partial void OnKOMKODChanged();
    partial void OnKOMNVNChanging(string value);
    partial void OnKOMNVNChanged();
    partial void OnSOEGMRKChanging(string value);
    partial void OnSOEGMRKChanged();
    partial void OnHISMDRChanging(decimal value);
    partial void OnHISMDRChanged();
    partial void OnCICSIDChanging(string value);
    partial void OnCICSIDChanged();
    partial void OnKUNDENRChanging(System.Nullable<decimal> value);
    partial void OnKUNDENRChanged();
    partial void OnLU62MRKChanging(string value);
    partial void OnLU62MRKChanged();
    partial void OnLUNAVNChanging(string value);
    partial void OnLUNAVNChanged();
    partial void OnLEVAJDTOChanging(System.Nullable<decimal> value);
    partial void OnLEVAJDTOChanged();
    partial void OnDB_VERSIONChanging(string value);
    partial void OnDB_VERSIONChanged();
    #endregion
		
		public Update()
		{
			OnCreated();
		}
		
		[global::System.Data.Linq.Mapping.ColumnAttribute(Storage="_DPRAJDTO", DbType="Decimal(9,0) NOT NULL")]
		public decimal DPRAJDTO
		{
			get
			{
				return this._DPRAJDTO;
			}
			set
			{
				if ((this._DPRAJDTO != value))
				{
					this.OnDPRAJDTOChanging(value);
					this.SendPropertyChanging();
					this._DPRAJDTO = value;
					this.SendPropertyChanged("DPRAJDTO");
					this.OnDPRAJDTOChanged();
				}
			}
		}
		
		[global::System.Data.Linq.Mapping.ColumnAttribute(Storage="_ANTAL", DbType="Decimal(9,0) NOT NULL")]
		public decimal ANTAL
		{
			get
			{
				return this._ANTAL;
			}
			set
			{
				if ((this._ANTAL != value))
				{
					this.OnANTALChanging(value);
					this.SendPropertyChanging();
					this._ANTAL = value;
					this.SendPropertyChanged("ANTAL");
					this.OnANTALChanged();
				}
			}
		}
		
		[global::System.Data.Linq.Mapping.ColumnAttribute(Storage="_KOMKOD", DbType="Decimal(5,0) NOT NULL")]
		public decimal KOMKOD
		{
			get
			{
				return this._KOMKOD;
			}
			set
			{
				if ((this._KOMKOD != value))
				{
					this.OnKOMKODChanging(value);
					this.SendPropertyChanging();
					this._KOMKOD = value;
					this.SendPropertyChanged("KOMKOD");
					this.OnKOMKODChanged();
				}
			}
		}
		
		[global::System.Data.Linq.Mapping.ColumnAttribute(Storage="_KOMNVN", DbType="VarChar(20)")]
		public string KOMNVN
		{
			get
			{
				return this._KOMNVN;
			}
			set
			{
				if ((this._KOMNVN != value))
				{
					this.OnKOMNVNChanging(value);
					this.SendPropertyChanging();
					this._KOMNVN = value;
					this.SendPropertyChanged("KOMNVN");
					this.OnKOMNVNChanged();
				}
			}
		}
		
		[global::System.Data.Linq.Mapping.ColumnAttribute(Storage="_SOEGMRK", DbType="VarChar(1)")]
		public string SOEGMRK
		{
			get
			{
				return this._SOEGMRK;
			}
			set
			{
				if ((this._SOEGMRK != value))
				{
					this.OnSOEGMRKChanging(value);
					this.SendPropertyChanging();
					this._SOEGMRK = value;
					this.SendPropertyChanged("SOEGMRK");
					this.OnSOEGMRKChanged();
				}
			}
		}
		
		[global::System.Data.Linq.Mapping.ColumnAttribute(Storage="_HISMDR", DbType="Decimal(3,0) NOT NULL")]
		public decimal HISMDR
		{
			get
			{
				return this._HISMDR;
			}
			set
			{
				if ((this._HISMDR != value))
				{
					this.OnHISMDRChanging(value);
					this.SendPropertyChanging();
					this._HISMDR = value;
					this.SendPropertyChanged("HISMDR");
					this.OnHISMDRChanged();
				}
			}
		}
		
		[global::System.Data.Linq.Mapping.ColumnAttribute(Storage="_CICSID", DbType="VarChar(20)")]
		public string CICSID
		{
			get
			{
				return this._CICSID;
			}
			set
			{
				if ((this._CICSID != value))
				{
					this.OnCICSIDChanging(value);
					this.SendPropertyChanging();
					this._CICSID = value;
					this.SendPropertyChanged("CICSID");
					this.OnCICSIDChanged();
				}
			}
		}
		
		[global::System.Data.Linq.Mapping.ColumnAttribute(Storage="_KUNDENR", DbType="Decimal(5,0)")]
		public System.Nullable<decimal> KUNDENR
		{
			get
			{
				return this._KUNDENR;
			}
			set
			{
				if ((this._KUNDENR != value))
				{
					this.OnKUNDENRChanging(value);
					this.SendPropertyChanging();
					this._KUNDENR = value;
					this.SendPropertyChanged("KUNDENR");
					this.OnKUNDENRChanged();
				}
			}
		}
		
		[global::System.Data.Linq.Mapping.ColumnAttribute(Storage="_LU62MRK", DbType="VarChar(1)")]
		public string LU62MRK
		{
			get
			{
				return this._LU62MRK;
			}
			set
			{
				if ((this._LU62MRK != value))
				{
					this.OnLU62MRKChanging(value);
					this.SendPropertyChanging();
					this._LU62MRK = value;
					this.SendPropertyChanged("LU62MRK");
					this.OnLU62MRKChanged();
				}
			}
		}
		
		[global::System.Data.Linq.Mapping.ColumnAttribute(Storage="_LUNAVN", DbType="VarChar(8)")]
		public string LUNAVN
		{
			get
			{
				return this._LUNAVN;
			}
			set
			{
				if ((this._LUNAVN != value))
				{
					this.OnLUNAVNChanging(value);
					this.SendPropertyChanging();
					this._LUNAVN = value;
					this.SendPropertyChanged("LUNAVN");
					this.OnLUNAVNChanged();
				}
			}
		}
		
		[global::System.Data.Linq.Mapping.ColumnAttribute(Storage="_LEVAJDTO", DbType="DECIMAL(9,0)")]
		public System.Nullable<decimal> LEVAJDTO
		{
			get
			{
				return this._LEVAJDTO;
			}
			set
			{
				if ((this._LEVAJDTO != value))
				{
					this.OnLEVAJDTOChanging(value);
					this.SendPropertyChanging();
					this._LEVAJDTO = value;
					this.SendPropertyChanged("LEVAJDTO");
					this.OnLEVAJDTOChanged();
				}
			}
		}
		
		[global::System.Data.Linq.Mapping.ColumnAttribute(Storage="_DB_VERSION", DbType="VarChar(30)", IsPrimaryKey=true)]
		public string DB_VERSION
		{
			get
			{
				return this._DB_VERSION;
			}
			set
			{
				if ((this._DB_VERSION != value))
				{
					this.OnDB_VERSIONChanging(value);
					this.SendPropertyChanging();
					this._DB_VERSION = value;
					this.SendPropertyChanged("DB_VERSION");
					this.OnDB_VERSIONChanged();
				}
			}
		}
		
		public event PropertyChangingEventHandler PropertyChanging;
		
		public event PropertyChangedEventHandler PropertyChanged;
		
		protected virtual void SendPropertyChanging()
		{
			if ((this.PropertyChanging != null))
			{
				this.PropertyChanging(this, emptyChangingEventArgs);
			}
		}
		
		protected virtual void SendPropertyChanged(String propertyName)
		{
			if ((this.PropertyChanged != null))
			{
				this.PropertyChanged(this, new PropertyChangedEventArgs(propertyName));
			}
		}
	}
}
#pragma warning restore 1591
