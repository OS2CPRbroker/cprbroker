﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace CprBroker.Data.Properties {
    using System;
    
    
    /// <summary>
    ///   A strongly-typed resource class, for looking up localized strings, etc.
    /// </summary>
    // This class was auto-generated by the StronglyTypedResourceBuilder
    // class via a tool like ResGen or Visual Studio.
    // To add or remove a member, edit your .ResX file then rerun ResGen
    // with the /str option, or rebuild your VS project.
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Resources.Tools.StronglyTypedResourceBuilder", "4.0.0.0")]
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    public class Resources {
        
        private static global::System.Resources.ResourceManager resourceMan;
        
        private static global::System.Globalization.CultureInfo resourceCulture;
        
        [global::System.Diagnostics.CodeAnalysis.SuppressMessageAttribute("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        internal Resources() {
        }
        
        /// <summary>
        ///   Returns the cached ResourceManager instance used by this class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        public static global::System.Resources.ResourceManager ResourceManager {
            get {
                if (object.ReferenceEquals(resourceMan, null)) {
                    global::System.Resources.ResourceManager temp = new global::System.Resources.ResourceManager("CprBroker.Data.Properties.Resources", typeof(Resources).Assembly);
                    resourceMan = temp;
                }
                return resourceMan;
            }
        }
        
        /// <summary>
        ///   Overrides the current thread's CurrentUICulture property for all
        ///   resource lookups using this strongly typed resource class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        public static global::System.Globalization.CultureInfo Culture {
            get {
                return resourceCulture;
            }
            set {
                resourceCulture = value;
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to IF NOT EXISTS (SELECT * FROM sys.tables WHERE name=N&apos;Activity&apos;)
        ///BEGIN
        ///	CREATE TABLE Activity(
        ///		ActivityId	UNIQUEIDENTIFIER			CONSTRAINT DF_Activity_ActivityId DEFAULT NEWID(),
        ///		ApplicationId UNIQUEIDENTIFIER NOT NULL CONSTRAINT FK_Activity_Application REFERENCES [Application](ApplicationId),
        ///		StartTS		DATETIME NOT NULL			CONSTRAINT DF_Activity_StartTS	  DEFAULT GETDATE(),
        ///		UserToken	VARCHAR(250) NULL,
        ///		UserId		VARCHAR(250) NULL,
        ///		MethodName	VARCHAR(250) NULL,
        ///
        ///		CONSTRAINT PK_Activity PRIMAR [rest of string was truncated]&quot;;.
        /// </summary>
        public static string Activity_Sql {
            get {
                return ResourceManager.GetString("Activity_Sql", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to ApplicationId;Name;Token;RegistrationDate;IsApproved;ApprovedDate
        ///{3E9890FF-0038-42A4-987A-99B63E8BC865};Base Application;07059250-E448-4040-B695-9C03F9E59E38;2009-06-25;True;
        ///{C98F9BE7-2DDE-404a-BAB5-5A7B1BBC3063};Event Broker;FCD568A0-8F18-4b6f-8691-C09239F158F3;2011-01-01;True;
        ///{4A78A5C8-B39B-41B9-9707-5782DAA56E2A};CPR Business Application Demo;5f8b7af5-422e-46bb-9273-5e244dc37505;2011-01-01;True;.
        /// </summary>
        public static string Application_Csv {
            get {
                return ResourceManager.GetString("Application_Csv", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to /****** Object:  Table [dbo].[Application]    Script Date: 11/21/2013 10:16:51 ******/
        ///SET ANSI_NULLS ON
        ///GO
        ///SET QUOTED_IDENTIFIER ON
        ///GO
        ///IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N&apos;[dbo].[Application]&apos;) AND type in (N&apos;U&apos;))
        ///BEGIN
        ///CREATE TABLE [dbo].[Application](
        ///	[ApplicationId] [uniqueidentifier] NOT NULL,
        ///	[Name] [nvarchar](100) NOT NULL,
        ///	[Token] [varchar](50) NOT NULL,
        ///	[RegistrationDate] [datetime] NOT NULL,
        ///	[IsApproved] [bit] NOT NULL,
        ///	[ApprovedDate] [datetime] [rest of string was truncated]&quot;;.
        /// </summary>
        public static string Application_Sql {
            get {
                return ResourceManager.GetString("Application_Sql", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to IntervalMilliseconds;Name;CallThreshold;CostThreshold;LastChecked
        ///3600000;Hour;;;
        ///86400000;Day;;;
        ///604800000;Week;;;
        ///2592000000;Month;;;.
        /// </summary>
        public static string BudgetInterval_Csv {
            get {
                return ResourceManager.GetString("BudgetInterval_Csv", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N&apos;[dbo].[BudgetInterval]&apos;) AND type in (N&apos;U&apos;))
        ///BEGIN
        ///    CREATE TABLE [dbo].[BudgetInterval](
        ///	    [IntervalMilliseconds] [bigint] NOT NULL
        ///            CONSTRAINT [PK_BudgetInterval] PRIMARY KEY CLUSTERED ([IntervalMilliseconds] ASC),
        ///	    [Name] [varchar](50) NOT NULL,
        ///	    [CallThreshold] [int] NULL,
        ///	    [CostThreshold] [decimal](18, 4) NULL,
        ///	    [LastChecked] [datetime] NULL
        ///    ) ON [PRIMARY]
        ///END
        ///
        ///GO
        ///.
        /// </summary>
        public static string BudgetInterval_Sql {
            get {
                return ResourceManager.GetString("BudgetInterval_Sql", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N&apos;[dbo].[DataProvider]&apos;) AND type in (N&apos;U&apos;))
        ///BEGIN
        ///    CREATE TABLE [dbo].[DataProvider](
        ///	    [DataProviderId] [uniqueidentifier] NOT NULL
        ///            CONSTRAINT [PK_DataProvider] PRIMARY KEY CLUSTERED ([DataProviderId] ASC),
        ///	    [TypeName] [varchar](250) NOT NULL,
        ///	    [Ordinal] [int] NOT NULL,
        ///	    [Data] [image] NULL,
        ///	    [IsExternal] [bit] NOT NULL,
        ///	    [IsEnabled] [bit] NOT NULL
        ///    ) ON [PRIMARY] 
        ///END
        ///GO
        ///.
        /// </summary>
        public static string DataProvider_Sql {
            get {
                return ResourceManager.GetString("DataProvider_Sql", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N&apos;[dbo].[DataProviderCall]&apos;) AND type in (N&apos;U&apos;))
        ///BEGIN
        ///    CREATE TABLE [dbo].[DataProviderCall](
        ///	    [DataProviderCallId] [uniqueidentifier] NOT NULL
        ///            CONSTRAINT [DF_DataProviderCall_DataProviderCallId]  DEFAULT (newid())
        ///            CONSTRAINT [PK_DataProviderCall] PRIMARY KEY NONCLUSTERED ([DataProviderCallId] ASC),
        ///	    [ActivityId] [uniqueidentifier] NOT NULL,
        ///	    [CallTime] [datetime] NOT NULL
        ///            CONSTRA [rest of string was truncated]&quot;;.
        /// </summary>
        public static string DataProviderCall_Sql {
            get {
                return ResourceManager.GetString("DataProviderCall_Sql", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to /****** Object:  Table [dbo].[LogEntry]    Script Date: 11/21/2013 10:16:51 ******/
        ///SET ANSI_NULLS ON
        ///GO
        ///SET QUOTED_IDENTIFIER ON
        ///GO
        ///IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N&apos;[dbo].[LogEntry]&apos;) AND type in (N&apos;U&apos;))
        ///BEGIN
        ///CREATE TABLE [dbo].[LogEntry](
        ///	[LogEntryId] [uniqueidentifier] NOT NULL,
        ///	[LogTypeId] [int] NOT NULL,
        ///	[ApplicationId] [uniqueidentifier] NULL,
        ///	[UserToken] [varchar](250) NULL,
        ///	[UserId] [varchar](250) NULL,
        ///	[MethodName] [varchar](250) NULL,
        ///	[Te [rest of string was truncated]&quot;;.
        /// </summary>
        public static string LogEntry_Sql {
            get {
                return ResourceManager.GetString("LogEntry_Sql", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to LogTypeId;Name
        ///1;Critical
        ///2;Error
        ///4;Warning
        ///8;Information
        ///16;Verbose
        ///256;Start
        ///512;Stop
        ///1024;Suspend
        ///2048;Resume
        ///4096;Transfer
        ///.
        /// </summary>
        public static string LogType_Csv {
            get {
                return ResourceManager.GetString("LogType_Csv", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to /****** Object:  Table [dbo].[LogType]    Script Date: 11/21/2013 10:16:51 ******/
        ///SET ANSI_NULLS ON
        ///GO
        ///SET QUOTED_IDENTIFIER ON
        ///GO
        ///IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N&apos;[dbo].[LogType]&apos;) AND type in (N&apos;U&apos;))
        ///BEGIN
        ///CREATE TABLE [dbo].[LogType](
        ///	[LogTypeId] [int] NOT NULL,
        ///	[Name] [varchar](50) NOT NULL,
        /// CONSTRAINT [PK_LogType] PRIMARY KEY CLUSTERED 
        ///(
        ///	[LogTypeId] ASC
        ///)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  [rest of string was truncated]&quot;;.
        /// </summary>
        public static string LogType_Sql {
            get {
                return ResourceManager.GetString("LogType_Sql", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to IF NOT EXISTS (SELECT * FROM sys.tables WHERE name=N&apos;Operation&apos;)
        ///BEGIN
        ///	CREATE TABLE Operation(
        ///		OperationId UNIQUEIDENTIFIER,
        ///		ActivityId UNIQUEIDENTIFIER NOT NULL,
        ///		OperationTypeId INT NOT NULL,
        ///		OperationKey VARCHAR(250) NOT NULL,
        ///		CONSTRAINT PK_Operation PRIMARY KEY NONCLUSTERED (OperationId),
        ///		CONSTRAINT FK_Operation_OperationType FOREIGN KEY (OperationTypeId) REFERENCES OperationType (OperationTypeId),
        ///		CONSTRAINT FK_Operation_Activity      FOREIGN KEY (ActivityId)      REFERENCES Acti [rest of string was truncated]&quot;;.
        /// </summary>
        public static string Operation_Sql {
            get {
                return ResourceManager.GetString("Operation_Sql", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Name;OperationTypeId
        ///Generic;1
        ///Read;2
        ///Search;3
        ///GetUuid;4
        ///ReadPeriod;5
        ///PutSubscription;6
        ///Subscribe;8
        ///Unsubscribe;9
        ///ListSubscriptions;10.
        /// </summary>
        public static string OperationType_Csv {
            get {
                return ResourceManager.GetString("OperationType_Csv", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to IF NOT EXISTS (SELECT * FROM sys.tables WHERE name=N&apos;OperationType&apos;)
        ///BEGIN
        ///	CREATE TABLE OperationType(
        ///		OperationTypeId INT,
        ///		Name VARCHAR(250),
        ///		CONSTRAINT PK_OperationType PRIMARY KEY (OperationTypeId)
        ///	)ON [PRIMARY] 
        ///END
        ///GO.
        /// </summary>
        public static string OperationType_Sql {
            get {
                return ResourceManager.GetString("OperationType_Sql", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N&apos;[dbo].[Queue]&apos;) AND type in (N&apos;U&apos;))
        ///BEGIN
        ///    CREATE TABLE [dbo].[Queue](
        ///	    [QueueId] [uniqueidentifier] NOT NULL
        ///            CONSTRAINT [PK_Queue] PRIMARY KEY CLUSTERED ([QueueId] ASC)
        ///            CONSTRAINT [DF_Queue_QueueId] DEFAULT NEWID(),
        ///	    [TypeId] [int] NULL,
        ///	    [TypeName] [varchar](250) NOT NULL,
        ///	    [BatchSize] [int] NOT NULL,
        ///	    [MaxRetry] [int] NOT NULL,
        ///	    [EncryptedData] [varbinary](max) NULL
        ///    ) O [rest of string was truncated]&quot;;.
        /// </summary>
        public static string Queue_Sql {
            get {
                return ResourceManager.GetString("Queue_Sql", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N&apos;[dbo].[QueueItem]&apos;) AND type in (N&apos;U&apos;))
        ///BEGIN
        ///    CREATE TABLE [dbo].[QueueItem](
        ///	    [QueueItemId]   int IDENTITY(1,1)   NOT NULL CONSTRAINT [PK_QueueItem]              PRIMARY KEY CLUSTERED ([QueueItemId] ASC),
        ///	    [QueueId]       uniqueidentifier    NOT NULL CONSTRAINT [FK_QueueItem_Queue]        FOREIGN KEY ([QueueId]) REFERENCES [dbo].[Queue] ([QueueId]) ON UPDATE CASCADE ON DELETE CASCADE,
        ///	    [ItemKey]       varchar(50)    [rest of string was truncated]&quot;;.
        /// </summary>
        public static string QueueItem_Sql {
            get {
                return ResourceManager.GetString("QueueItem_Sql", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N&apos;[dbo].[Semaphore]&apos;) AND type in (N&apos;U&apos;))
        ///BEGIN    
        ///    CREATE TABLE [dbo].[Semaphore](
        ///	    [SemaphoreId] [uniqueidentifier] NOT NULL 
        ///            CONSTRAINT [PK_Semaphore] PRIMARY KEY CLUSTERED ([SemaphoreId] ASC)
        ///            CONSTRAINT [DF_Semaphore_SemaphoreId]  DEFAULT (newid()),
        ///	    [CreatedDate] [datetime] NOT NULL,
        ///		[WaitCount] INT DEFAULT 1 NULL,
        ///	    [SignaledDate] [datetime] NULL,
        ///    ) ON [PRIMARY]
        ///END
        ///GO.
        /// </summary>
        public static string Semaphore_Sql {
            get {
                return ResourceManager.GetString("Semaphore_Sql", resourceCulture);
            }
        }
    }
}
