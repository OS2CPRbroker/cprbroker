﻿#pragma warning disable 1591
//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:2.0.50727.3615
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace CprBroker.DAL.Applications
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
	
	
	public partial class ApplicationDataContext : System.Data.Linq.DataContext
	{
		
		private static System.Data.Linq.Mapping.MappingSource mappingSource = new AttributeMappingSource();
		
    #region Extensibility Method Definitions
    partial void OnCreated();
    partial void InsertLogType(LogType instance);
    partial void UpdateLogType(LogType instance);
    partial void DeleteLogType(LogType instance);
    partial void InsertApplication(Application instance);
    partial void UpdateApplication(Application instance);
    partial void DeleteApplication(Application instance);
    partial void InsertLogEntry(LogEntry instance);
    partial void UpdateLogEntry(LogEntry instance);
    partial void DeleteLogEntry(LogEntry instance);
    #endregion
		
		public ApplicationDataContext(string connection) : 
				base(connection, mappingSource)
		{
			OnCreated();
		}
		
		public ApplicationDataContext(System.Data.IDbConnection connection) : 
				base(connection, mappingSource)
		{
			OnCreated();
		}
		
		public ApplicationDataContext(string connection, System.Data.Linq.Mapping.MappingSource mappingSource) : 
				base(connection, mappingSource)
		{
			OnCreated();
		}
		
		public ApplicationDataContext(System.Data.IDbConnection connection, System.Data.Linq.Mapping.MappingSource mappingSource) : 
				base(connection, mappingSource)
		{
			OnCreated();
		}
		
		public System.Data.Linq.Table<LogType> LogTypes
		{
			get
			{
				return this.GetTable<LogType>();
			}
		}
		
		public System.Data.Linq.Table<Application> Applications
		{
			get
			{
				return this.GetTable<Application>();
			}
		}
		
		public System.Data.Linq.Table<LogEntry> LogEntries
		{
			get
			{
				return this.GetTable<LogEntry>();
			}
		}
	}
	
	[Table(Name="dbo.LogType")]
	public partial class LogType : INotifyPropertyChanging, INotifyPropertyChanged
	{
		
		private static PropertyChangingEventArgs emptyChangingEventArgs = new PropertyChangingEventArgs(String.Empty);
		
		private int _LogTypeId;
		
		private string _Name;
		
		private EntitySet<LogEntry> _LogEntries;
		
    #region Extensibility Method Definitions
    partial void OnLoaded();
    partial void OnValidate(System.Data.Linq.ChangeAction action);
    partial void OnCreated();
    partial void OnLogTypeIdChanging(int value);
    partial void OnLogTypeIdChanged();
    partial void OnNameChanging(string value);
    partial void OnNameChanged();
    #endregion
		
		public LogType()
		{
			this._LogEntries = new EntitySet<LogEntry>(new Action<LogEntry>(this.attach_LogEntries), new Action<LogEntry>(this.detach_LogEntries));
			OnCreated();
		}
		
		[Column(Storage="_LogTypeId", DbType="Int NOT NULL", IsPrimaryKey=true)]
		public int LogTypeId
		{
			get
			{
				return this._LogTypeId;
			}
			set
			{
				if ((this._LogTypeId != value))
				{
					this.OnLogTypeIdChanging(value);
					this.SendPropertyChanging();
					this._LogTypeId = value;
					this.SendPropertyChanged("LogTypeId");
					this.OnLogTypeIdChanged();
				}
			}
		}
		
		[Column(Storage="_Name", DbType="VarChar(50) NOT NULL", CanBeNull=false)]
		public string Name
		{
			get
			{
				return this._Name;
			}
			set
			{
				if ((this._Name != value))
				{
					this.OnNameChanging(value);
					this.SendPropertyChanging();
					this._Name = value;
					this.SendPropertyChanged("Name");
					this.OnNameChanged();
				}
			}
		}
		
		[Association(Name="LogType_LogEntry", Storage="_LogEntries", ThisKey="LogTypeId", OtherKey="LogTypeId")]
		public EntitySet<LogEntry> LogEntries
		{
			get
			{
				return this._LogEntries;
			}
			set
			{
				this._LogEntries.Assign(value);
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
		
		private void attach_LogEntries(LogEntry entity)
		{
			this.SendPropertyChanging();
			entity.LogType = this;
		}
		
		private void detach_LogEntries(LogEntry entity)
		{
			this.SendPropertyChanging();
			entity.LogType = null;
		}
	}
	
	[Table(Name="dbo.Application")]
	public partial class Application : INotifyPropertyChanging, INotifyPropertyChanged
	{
		
		private static PropertyChangingEventArgs emptyChangingEventArgs = new PropertyChangingEventArgs(String.Empty);
		
		private System.Guid _ApplicationId;
		
		private string _Name;
		
		private string _Token;
		
		private System.DateTime _RegistrationDate;
		
		private bool _IsApproved;
		
		private System.Nullable<System.DateTime> _ApprovedDate;
		
		private EntitySet<LogEntry> _LogEntries;
		
    #region Extensibility Method Definitions
    partial void OnLoaded();
    partial void OnValidate(System.Data.Linq.ChangeAction action);
    partial void OnCreated();
    partial void OnApplicationIdChanging(System.Guid value);
    partial void OnApplicationIdChanged();
    partial void OnNameChanging(string value);
    partial void OnNameChanged();
    partial void OnTokenChanging(string value);
    partial void OnTokenChanged();
    partial void OnRegistrationDateChanging(System.DateTime value);
    partial void OnRegistrationDateChanged();
    partial void OnIsApprovedChanging(bool value);
    partial void OnIsApprovedChanged();
    partial void OnApprovedDateChanging(System.Nullable<System.DateTime> value);
    partial void OnApprovedDateChanged();
    #endregion
		
		public Application()
		{
			this._LogEntries = new EntitySet<LogEntry>(new Action<LogEntry>(this.attach_LogEntries), new Action<LogEntry>(this.detach_LogEntries));
			OnCreated();
		}
		
		[Column(Storage="_ApplicationId", DbType="UniqueIdentifier NOT NULL", IsPrimaryKey=true)]
		public System.Guid ApplicationId
		{
			get
			{
				return this._ApplicationId;
			}
			set
			{
				if ((this._ApplicationId != value))
				{
					this.OnApplicationIdChanging(value);
					this.SendPropertyChanging();
					this._ApplicationId = value;
					this.SendPropertyChanged("ApplicationId");
					this.OnApplicationIdChanged();
				}
			}
		}
		
		[Column(Storage="_Name", DbType="NVarChar(100) NOT NULL", CanBeNull=false)]
		public string Name
		{
			get
			{
				return this._Name;
			}
			set
			{
				if ((this._Name != value))
				{
					this.OnNameChanging(value);
					this.SendPropertyChanging();
					this._Name = value;
					this.SendPropertyChanged("Name");
					this.OnNameChanged();
				}
			}
		}
		
		[Column(Storage="_Token", DbType="VarChar(50) NOT NULL", CanBeNull=false)]
		public string Token
		{
			get
			{
				return this._Token;
			}
			set
			{
				if ((this._Token != value))
				{
					this.OnTokenChanging(value);
					this.SendPropertyChanging();
					this._Token = value;
					this.SendPropertyChanged("Token");
					this.OnTokenChanged();
				}
			}
		}
		
		[Column(Storage="_RegistrationDate", DbType="DateTime NOT NULL")]
		public System.DateTime RegistrationDate
		{
			get
			{
				return this._RegistrationDate;
			}
			set
			{
				if ((this._RegistrationDate != value))
				{
					this.OnRegistrationDateChanging(value);
					this.SendPropertyChanging();
					this._RegistrationDate = value;
					this.SendPropertyChanged("RegistrationDate");
					this.OnRegistrationDateChanged();
				}
			}
		}
		
		[Column(Storage="_IsApproved", DbType="Bit NOT NULL")]
		public bool IsApproved
		{
			get
			{
				return this._IsApproved;
			}
			set
			{
				if ((this._IsApproved != value))
				{
					this.OnIsApprovedChanging(value);
					this.SendPropertyChanging();
					this._IsApproved = value;
					this.SendPropertyChanged("IsApproved");
					this.OnIsApprovedChanged();
				}
			}
		}
		
		[Column(Storage="_ApprovedDate", DbType="DateTime")]
		public System.Nullable<System.DateTime> ApprovedDate
		{
			get
			{
				return this._ApprovedDate;
			}
			set
			{
				if ((this._ApprovedDate != value))
				{
					this.OnApprovedDateChanging(value);
					this.SendPropertyChanging();
					this._ApprovedDate = value;
					this.SendPropertyChanged("ApprovedDate");
					this.OnApprovedDateChanged();
				}
			}
		}
		
		[Association(Name="Application_LogEntry", Storage="_LogEntries", ThisKey="ApplicationId", OtherKey="ApplicationId")]
		public EntitySet<LogEntry> LogEntries
		{
			get
			{
				return this._LogEntries;
			}
			set
			{
				this._LogEntries.Assign(value);
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
		
		private void attach_LogEntries(LogEntry entity)
		{
			this.SendPropertyChanging();
			entity.Application = this;
		}
		
		private void detach_LogEntries(LogEntry entity)
		{
			this.SendPropertyChanging();
			entity.Application = null;
		}
	}
	
	[Table(Name="dbo.LogEntry")]
	public partial class LogEntry : INotifyPropertyChanging, INotifyPropertyChanged
	{
		
		private static PropertyChangingEventArgs emptyChangingEventArgs = new PropertyChangingEventArgs(String.Empty);
		
		private System.Guid _LogEntryId;
		
		private int _LogTypeId;
		
		private System.Nullable<System.Guid> _ApplicationId;
		
		private string _UserToken;
		
		private string _UserId;
		
		private string _MethodName;
		
		private string _Text;
		
		private string _DataObjectType;
		
		private string _DataObjectXml;
		
		private System.DateTime _LogDate;
		
		private EntityRef<Application> _Application;
		
		private EntityRef<LogType> _LogType;
		
    #region Extensibility Method Definitions
    partial void OnLoaded();
    partial void OnValidate(System.Data.Linq.ChangeAction action);
    partial void OnCreated();
    partial void OnLogEntryIdChanging(System.Guid value);
    partial void OnLogEntryIdChanged();
    partial void OnLogTypeIdChanging(int value);
    partial void OnLogTypeIdChanged();
    partial void OnApplicationIdChanging(System.Nullable<System.Guid> value);
    partial void OnApplicationIdChanged();
    partial void OnUserTokenChanging(string value);
    partial void OnUserTokenChanged();
    partial void OnUserIdChanging(string value);
    partial void OnUserIdChanged();
    partial void OnMethodNameChanging(string value);
    partial void OnMethodNameChanged();
    partial void OnTextChanging(string value);
    partial void OnTextChanged();
    partial void OnDataObjectTypeChanging(string value);
    partial void OnDataObjectTypeChanged();
    partial void OnDataObjectXmlChanging(string value);
    partial void OnDataObjectXmlChanged();
    partial void OnLogDateChanging(System.DateTime value);
    partial void OnLogDateChanged();
    #endregion
		
		public LogEntry()
		{
			this._Application = default(EntityRef<Application>);
			this._LogType = default(EntityRef<LogType>);
			OnCreated();
		}
		
		[Column(Storage="_LogEntryId", DbType="UniqueIdentifier NOT NULL", IsPrimaryKey=true)]
		public System.Guid LogEntryId
		{
			get
			{
				return this._LogEntryId;
			}
			set
			{
				if ((this._LogEntryId != value))
				{
					this.OnLogEntryIdChanging(value);
					this.SendPropertyChanging();
					this._LogEntryId = value;
					this.SendPropertyChanged("LogEntryId");
					this.OnLogEntryIdChanged();
				}
			}
		}
		
		[Column(Storage="_LogTypeId", DbType="Int NOT NULL")]
		public int LogTypeId
		{
			get
			{
				return this._LogTypeId;
			}
			set
			{
				if ((this._LogTypeId != value))
				{
					if (this._LogType.HasLoadedOrAssignedValue)
					{
						throw new System.Data.Linq.ForeignKeyReferenceAlreadyHasValueException();
					}
					this.OnLogTypeIdChanging(value);
					this.SendPropertyChanging();
					this._LogTypeId = value;
					this.SendPropertyChanged("LogTypeId");
					this.OnLogTypeIdChanged();
				}
			}
		}
		
		[Column(Storage="_ApplicationId", DbType="UniqueIdentifier")]
		public System.Nullable<System.Guid> ApplicationId
		{
			get
			{
				return this._ApplicationId;
			}
			set
			{
				if ((this._ApplicationId != value))
				{
					if (this._Application.HasLoadedOrAssignedValue)
					{
						throw new System.Data.Linq.ForeignKeyReferenceAlreadyHasValueException();
					}
					this.OnApplicationIdChanging(value);
					this.SendPropertyChanging();
					this._ApplicationId = value;
					this.SendPropertyChanged("ApplicationId");
					this.OnApplicationIdChanged();
				}
			}
		}
		
		[Column(Storage="_UserToken", DbType="VarChar(250)")]
		public string UserToken
		{
			get
			{
				return this._UserToken;
			}
			set
			{
				if ((this._UserToken != value))
				{
					this.OnUserTokenChanging(value);
					this.SendPropertyChanging();
					this._UserToken = value;
					this.SendPropertyChanged("UserToken");
					this.OnUserTokenChanged();
				}
			}
		}
		
		[Column(Storage="_UserId", DbType="VarChar(250)")]
		public string UserId
		{
			get
			{
				return this._UserId;
			}
			set
			{
				if ((this._UserId != value))
				{
					this.OnUserIdChanging(value);
					this.SendPropertyChanging();
					this._UserId = value;
					this.SendPropertyChanged("UserId");
					this.OnUserIdChanged();
				}
			}
		}
		
		[Column(Storage="_MethodName", DbType="VarChar(250)")]
		public string MethodName
		{
			get
			{
				return this._MethodName;
			}
			set
			{
				if ((this._MethodName != value))
				{
					this.OnMethodNameChanging(value);
					this.SendPropertyChanging();
					this._MethodName = value;
					this.SendPropertyChanged("MethodName");
					this.OnMethodNameChanged();
				}
			}
		}
		
		[Column(Storage="_Text", DbType="NVarChar(MAX)")]
		public string Text
		{
			get
			{
				return this._Text;
			}
			set
			{
				if ((this._Text != value))
				{
					this.OnTextChanging(value);
					this.SendPropertyChanging();
					this._Text = value;
					this.SendPropertyChanged("Text");
					this.OnTextChanged();
				}
			}
		}
		
		[Column(Storage="_DataObjectType", DbType="VarChar(250)")]
		public string DataObjectType
		{
			get
			{
				return this._DataObjectType;
			}
			set
			{
				if ((this._DataObjectType != value))
				{
					this.OnDataObjectTypeChanging(value);
					this.SendPropertyChanging();
					this._DataObjectType = value;
					this.SendPropertyChanged("DataObjectType");
					this.OnDataObjectTypeChanged();
				}
			}
		}
		
		[Column(Storage="_DataObjectXml", DbType="NText", UpdateCheck=UpdateCheck.Never)]
		public string DataObjectXml
		{
			get
			{
				return this._DataObjectXml;
			}
			set
			{
				if ((this._DataObjectXml != value))
				{
					this.OnDataObjectXmlChanging(value);
					this.SendPropertyChanging();
					this._DataObjectXml = value;
					this.SendPropertyChanged("DataObjectXml");
					this.OnDataObjectXmlChanged();
				}
			}
		}
		
		[Column(Storage="_LogDate", DbType="DateTime NOT NULL")]
		public System.DateTime LogDate
		{
			get
			{
				return this._LogDate;
			}
			set
			{
				if ((this._LogDate != value))
				{
					this.OnLogDateChanging(value);
					this.SendPropertyChanging();
					this._LogDate = value;
					this.SendPropertyChanged("LogDate");
					this.OnLogDateChanged();
				}
			}
		}
		
		[Association(Name="Application_LogEntry", Storage="_Application", ThisKey="ApplicationId", OtherKey="ApplicationId", IsForeignKey=true)]
		public Application Application
		{
			get
			{
				return this._Application.Entity;
			}
			set
			{
				Application previousValue = this._Application.Entity;
				if (((previousValue != value) 
							|| (this._Application.HasLoadedOrAssignedValue == false)))
				{
					this.SendPropertyChanging();
					if ((previousValue != null))
					{
						this._Application.Entity = null;
						previousValue.LogEntries.Remove(this);
					}
					this._Application.Entity = value;
					if ((value != null))
					{
						value.LogEntries.Add(this);
						this._ApplicationId = value.ApplicationId;
					}
					else
					{
						this._ApplicationId = default(Nullable<System.Guid>);
					}
					this.SendPropertyChanged("Application");
				}
			}
		}
		
		[Association(Name="LogType_LogEntry", Storage="_LogType", ThisKey="LogTypeId", OtherKey="LogTypeId", IsForeignKey=true)]
		public LogType LogType
		{
			get
			{
				return this._LogType.Entity;
			}
			set
			{
				LogType previousValue = this._LogType.Entity;
				if (((previousValue != value) 
							|| (this._LogType.HasLoadedOrAssignedValue == false)))
				{
					this.SendPropertyChanging();
					if ((previousValue != null))
					{
						this._LogType.Entity = null;
						previousValue.LogEntries.Remove(this);
					}
					this._LogType.Entity = value;
					if ((value != null))
					{
						value.LogEntries.Add(this);
						this._LogTypeId = value.LogTypeId;
					}
					else
					{
						this._LogTypeId = default(int);
					}
					this.SendPropertyChanged("LogType");
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
