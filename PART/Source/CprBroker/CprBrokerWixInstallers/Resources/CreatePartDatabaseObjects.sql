SET ANSI_PADDING ON
GO


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[RelationshipType]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[RelationshipType](
	[RelationshipTypeId] [int] NOT NULL,
	[ForwardName] [varchar](50) NOT NULL,
 CONSTRAINT [PK_RelationshipType] PRIMARY KEY CLUSTERED 
(
	[RelationshipTypeId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Extract]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[Extract](
	[ExtractId] [uniqueidentifier] NOT NULL,
	[Filename] [nvarchar](max) NOT NULL,
	[ExtractDate] [datetime] NOT NULL,
	[ImportDate] [datetime] NOT NULL,
	[StartRecord] [nvarchar](max) NOT NULL,
	[EndRecord] [nvarchar](max) NOT NULL,
	[Ready] [bit] NOT NULL,
	[ProcessedLines] [bigint] NULL
 CONSTRAINT [PK_Extract] PRIMARY KEY CLUSTERED 
(
	[ExtractId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dbo].[Extract]') AND name = N'IX_Extract_ExtractDate')
CREATE NONCLUSTERED INDEX [IX_Extract_ExtractDate] ON [dbo].[Extract] 
(
	[ExtractDate] DESC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CountrySchemeType]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[CountrySchemeType](
	[Id] [int] NOT NULL,
	[Name] [varchar](50) NULL,
 CONSTRAINT [PK_CountrySchemeType] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[AddressCoordinateQualityType]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[AddressCoordinateQualityType](
	[Code] [char](1) NOT NULL,
	[Name] [varchar](50) NOT NULL,
 CONSTRAINT [PK_CoordinateQualityType] PRIMARY KEY CLUSTERED 
(
	[Code] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Address]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[Address](
	[AddressId] [uniqueidentifier] NOT NULL,
	[Note] [varchar](200) NULL,
	[IsUnknown] [bit] NOT NULL,
 CONSTRAINT [PK_Address_1] PRIMARY KEY CLUSTERED 
(
	[AddressId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ActorRef]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[ActorRef](
	[ActorRefId] [uniqueidentifier] NOT NULL,
	[Type] [int] NOT NULL,
	[Value] [varchar](50) NULL,
 CONSTRAINT [PK_ActorRef] PRIMARY KEY CLUSTERED 
(
	[ActorRefId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CivilStatusCodeType]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[CivilStatusCodeType](
	[CivilStatusCodeTypeId] [int] NOT NULL,
	[Name] [varchar](50) NOT NULL,
 CONSTRAINT [PK_CivilStatusCodeType] PRIMARY KEY CLUSTERED 
(
	[CivilStatusCodeTypeId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Authority]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[Authority](
	[AuthorityCode] [varchar](4) NOT NULL,
	[AuthorityType] [varchar](2) NOT NULL,
	[AuthorityGroup] [char](10) NOT NULL,
	[UpdateTime] [datetime] NOT NULL,
	[AuthorityPhone] [varchar](8) NOT NULL,
	[StartDate] [datetime] NOT NULL,
	[EndDate] [datetime] NULL,
	[AuthorityName] [nvarchar](20) NULL,
	[Address] [nvarchar](34) NULL,
	[AddressLine1] [nvarchar](34) NULL,
	[AddressLine2] [nvarchar](34) NULL,
	[AddressLine3] [nvarchar](34) NULL,
	[AddressLine4] [nvarchar](34) NULL,
	[Telefax] [varchar](8) NULL,
	[FullName] [nvarchar](60) NULL,
	[Email] [nvarchar](100) NULL,
	[Alpha2CountryCode] [char](2) NOT NULL,
	[Alpha3CountryCode] [char](3) NOT NULL,
	[NumericCountryCode] [char](3) NOT NULL,
 CONSTRAINT [PK_Authority] PRIMARY KEY CLUSTERED 
(
	[AuthorityCode] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Application]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[Application](
	[ApplicationId] [uniqueidentifier] NOT NULL,
	[Name] [nvarchar](100) NOT NULL,
	[Token] [varchar](50) NOT NULL,
	[RegistrationDate] [datetime] NOT NULL,
	[IsApproved] [bit] NOT NULL,
	[ApprovedDate] [datetime] NULL,
 CONSTRAINT [PK_Application] PRIMARY KEY CLUSTERED 
(
	[ApplicationId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Country]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[Country](
	[Alpha2Code] [varchar](2) NOT NULL,
	[Alpha3Code] [varchar](3) NOT NULL,
	[NumericCode] [int] NOT NULL,
	[CountryName] [nvarchar](60) NOT NULL,
	[Description] [nvarchar](50) NULL,
	[DanishCountryName] [nvarchar](60) NOT NULL,
	[DanishCountryName2] [nvarchar](50) NULL,
	[KmdCode] [int] NULL,
	[KmdCode2] [int] NULL,
	[KmdCode3] [int] NULL,
 CONSTRAINT [PK_Country] PRIMARY KEY CLUSTERED 
(
	[Alpha2Code] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO
IF NOT EXISTS (SELECT * FROM ::fn_listextendedproperty(N'Notes' , N'SCHEMA',N'dbo', N'TABLE',N'Country', NULL,NULL))
EXEC sys.sp_addextendedproperty @name=N'Notes', @value=N'This is a list of iso3166 Standard Country Codes' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'Country'
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ContactChannelType]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[ContactChannelType](
	[ContactChannelTypeId] [int] NOT NULL,
	[Name] [varchar](50) NOT NULL,
 CONSTRAINT [PK_ContactChannelType] PRIMARY KEY CLUSTERED 
(
	[ContactChannelTypeId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Gender]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[Gender](
	[GenderId] [int] NOT NULL,
	[GenderName] [nvarchar](50) NOT NULL,
 CONSTRAINT [PK_Gender] PRIMARY KEY CLUSTERED 
(
	[GenderId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PersonMapping]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[PersonMapping](
	[UUID] [uniqueidentifier] NOT NULL,
	[CprNumber] [varchar](10) NOT NULL,
 CONSTRAINT [PK_PersonMapping] PRIMARY KEY CLUSTERED 
(
	[UUID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY],
 CONSTRAINT [IX_PersonMapping] UNIQUE NONCLUSTERED 
(
	[CprNumber] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Person]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[Person](
	[UUID] [uniqueidentifier] NOT NULL,
	[UserInterfaceKeyText] [varchar](50) NOT NULL,
 CONSTRAINT [PK_Object] PRIMARY KEY CLUSTERED 
(
	[UUID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LogType]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[LogType](
	[LogTypeId] [int] NOT NULL,
	[Name] [varchar](50) NOT NULL,
 CONSTRAINT [PK_LogType] PRIMARY KEY CLUSTERED 
(
	[LogTypeId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LogEntry]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[LogEntry](
	[LogEntryId] [uniqueidentifier] NOT NULL,
	[LogTypeId] [int] NOT NULL,
	[ApplicationId] [uniqueidentifier] NULL,
	[UserToken] [varchar](250) NULL,
	[UserId] [varchar](250) NULL,
	[MethodName] [varchar](250) NULL,
	[Text] [nvarchar](max) NULL,
	[DataObjectType] [varchar](250) NULL,
	[DataObjectXml] [ntext] NULL,
	[LogDate] [datetime] NOT NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END
GO
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dbo].[LogEntry]') AND name = N'IX_LogEntry_LogDate')
CREATE CLUSTERED INDEX [IX_LogEntry_LogDate] ON [dbo].[LogEntry] 
(
	[LogDate] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dbo].[LogEntry]') AND name = N'PK_LogEntry')
CREATE UNIQUE NONCLUSTERED INDEX [PK_LogEntry] ON [dbo].[LogEntry] 
(
	[LogEntryId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LifeStatusCodeType]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[LifeStatusCodeType](
	[LifeStatusCodeTypeId] [int] NOT NULL,
	[Name] [varchar](50) NOT NULL,
 CONSTRAINT [PK_LifeStatusCodeType] PRIMARY KEY CLUSTERED 
(
	[LifeStatusCodeTypeId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LifecycleStatus]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[LifecycleStatus](
	[LifecycleStatusId] [int] NOT NULL,
	[LifecycleStatusName] [varchar](50) NULL,
 CONSTRAINT [PK_LifecycleStatus] PRIMARY KEY CLUSTERED 
(
	[LifecycleStatusId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DataProvider]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[DataProvider](
	[DataProviderId] [uniqueidentifier] NOT NULL,
	[TypeName] [varchar](250) NOT NULL,
	[Ordinal] [int] NOT NULL,
	[Data] [image] NULL,
	[IsExternal] [bit] NOT NULL,
	[IsEnabled] [bit] NOT NULL,
 CONSTRAINT [PK_DataProvider] PRIMARY KEY CLUSTERED 
(
	[DataProviderId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DataChangeEvent]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[DataChangeEvent](
	[DataChangeEventId] [uniqueidentifier] NOT NULL,
	[PersonUuid] [uniqueidentifier] NOT NULL,
	[PersonRegistrationId] [uniqueidentifier] NOT NULL,
	[ReceivedDate] [datetime] NOT NULL,
 CONSTRAINT [PK_DataChangeEvent] PRIMARY KEY CLUSTERED 
(
	[DataChangeEventId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ExtractItem]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[ExtractItem](
	[ExtractItemId] [uniqueidentifier] NOT NULL,
	[ExtractId] [uniqueidentifier] NOT NULL,
	[PNR] [varchar](10) NOT NULL,
	[RelationPNR] [varchar](10) NULL,
	[RelationPNR2] [varchar](10) NULL,
	[DataTypeCode] [varchar](10) NOT NULL,
	[Contents] [nvarchar](max) NOT NULL,
 CONSTRAINT [UK_ExtractItem_ExtractItemId] UNIQUE NONCLUSTERED 
(
	[ExtractItemId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dbo].[ExtractItem]') AND name = N'IX_ExtractItem_PNR_ExtractId')
CREATE CLUSTERED INDEX [IX_ExtractItem_PNR_ExtractId] ON [dbo].[ExtractItem] 
(
	[PNR] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dbo].[ExtractItem]') AND name = N'IX_ExtractItem_ExtractId')
CREATE NONCLUSTERED INDEX [IX_ExtractItem_ExtractId] ON [dbo].[ExtractItem] 
(
	[ExtractId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dbo].[ExtractItem]') AND name = N'IX_ExtractItem_RelationPNR')
CREATE NONCLUSTERED INDEX [IX_ExtractItem_RelationPNR] ON [dbo].[ExtractItem] 
(
	[RelationPNR] ASC
)
INCLUDE ( [DataTypeCode]) 
WHERE ([PNR] IS NOT NULL)
WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ExtractError]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[ExtractError](
	[ExtractErrorId] [uniqueidentifier] NOT NULL,
	[ExtractId] [uniqueidentifier] NOT NULL,
	[Contents] [nvarchar](157) NOT NULL,
 CONSTRAINT [PK_ExtractError] PRIMARY KEY CLUSTERED 
(
	[ExtractErrorId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PersonRegistration]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[PersonRegistration](
	[PersonRegistrationId] [uniqueidentifier] NOT NULL,
	[UUID] [uniqueidentifier] NOT NULL,
	[ActorRefId] [uniqueidentifier] NULL,
	[RegistrationDate] [datetime] NOT NULL,
	[BrokerUpdateDate] [datetime] NOT NULL,
	[CommentText] [varchar](50) NULL,
	[LifecycleStatusId] [int] NOT NULL,
	[Contents] [xml] NULL,
	[SourceObjects] [xml] NULL,
 CONSTRAINT [PK_Registration] PRIMARY KEY CLUSTERED 
(
	[PersonRegistrationId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ExtractPersonStaging]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[ExtractPersonStaging](
	[ExtractPersonStagingId] [uniqueidentifier] NOT NULL,
	[ExtractId] [uniqueidentifier] NOT NULL,
	[PNR] [varchar](10) NOT NULL,
 CONSTRAINT [PK_ExtractPersonStaging] PRIMARY KEY CLUSTERED 
(
	[ExtractPersonStagingId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ContactChannel]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[ContactChannel](
	[ContactChannelId] [uniqueidentifier] NOT NULL,
	[ContactChannelTypeId] [int] NOT NULL,
	[UsageLimits] [varchar](50) NULL,
	[Value] [varchar](200) NOT NULL,
	[CanSendSms] [bit] NULL,
	[Note] [varchar](50) NULL,
	[OtherNote] [varchar](50) NULL,
 CONSTRAINT [PK_ContactChannel] PRIMARY KEY CLUSTERED 
(
	[ContactChannelId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CountryRef]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[CountryRef](
	[CountryRefId] [uniqueidentifier] NOT NULL,
	[CountrySchemeTypeId] [int] NOT NULL,
	[Value] [varchar](50) NULL,
 CONSTRAINT [PK_CountryRef] PRIMARY KEY CLUSTERED 
(
	[CountryRefId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Effect]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[Effect](
	[EffectId] [uniqueidentifier] NOT NULL,
	[ActorRefId] [uniqueidentifier] NULL,
	[CommentText] [varchar](50) NULL,
	[FromDate] [datetime] NULL,
	[ToDate] [datetime] NULL,
 CONSTRAINT [PK_Effect] PRIMARY KEY CLUSTERED 
(
	[EffectId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DenmarkAddress]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[DenmarkAddress](
	[AddressId] [uniqueidentifier] NOT NULL,
	[SpecialRoadCode] [bit] NULL,
	[MunicipalityCode] [varchar](4) NULL,
	[StreetCode] [varchar](4) NULL,
	[MailDeliverySublocation] [varchar](50) NULL,
	[StreetName] [varchar](50) NULL,
	[StreetNameForAddressing] [varchar](50) NULL,
	[StreetBuildingIdentifier] [varchar](4) NULL,
	[FloorIdentifier] [varchar](50) NULL,
	[SuiteIdentifier] [varchar](50) NULL,
	[DistrictSubdivisionIdentifier] [varchar](50) NULL,
	[PostCodeIdentifier] [varchar](50) NULL,
	[DistrictName] [varchar](50) NULL,
	[CountryRefId] [uniqueidentifier] NULL,
 CONSTRAINT [PK_DenmarkAddress] PRIMARY KEY CLUSTERED 
(
	[AddressId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PersonAttributes]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[PersonAttributes](
	[PersonAttributesId] [uniqueidentifier] NOT NULL,
	[EffectId] [uniqueidentifier] NULL,
	[PersonRegistrationId] [uniqueidentifier] NOT NULL,
 CONSTRAINT [PK_PersonAttributes_1] PRIMARY KEY CLUSTERED 
(
	[PersonAttributesId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PersonState]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[PersonState](
	[PersonRegistrationId] [uniqueidentifier] NOT NULL,
 CONSTRAINT [PK_PersonState] PRIMARY KEY CLUSTERED 
(
	[PersonRegistrationId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PersonRelationship]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[PersonRelationship](
	[PersonRelationshipId] [uniqueidentifier] NOT NULL,
	[PersonRegistrationId] [uniqueidentifier] NOT NULL,
	[RelatedPersonUuid] [uniqueidentifier] NOT NULL,
	[RelationshipTypeId] [int] NOT NULL,
	[CommentText] [varchar](50) NULL,
	[EffectId] [uniqueidentifier] NULL,
 CONSTRAINT [PK_PersonRelationship_1] PRIMARY KEY CLUSTERED 
(
	[PersonRelationshipId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ForeignAddress]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[ForeignAddress](
	[AddressId] [uniqueidentifier] NOT NULL,
	[FirstLine] [varchar](50) NULL,
	[SecondLine] [varchar](50) NULL,
	[ThirdLine] [varchar](50) NULL,
	[FourthLine] [varchar](50) NULL,
	[FifthLine] [varchar](50) NULL,
	[LocationDescription] [varchar](50) NULL,
	[CountryRefId] [uniqueidentifier] NULL,
 CONSTRAINT [PK_ForeignAddress] PRIMARY KEY CLUSTERED 
(
	[AddressId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[UnknownCitizenData]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[UnknownCitizenData](
	[PersonAttributesId] [uniqueidentifier] NOT NULL,
	[CprNumber] [varchar](50) NULL,
 CONSTRAINT [PK_UnknownCitizenData] PRIMARY KEY CLUSTERED 
(
	[PersonAttributesId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PersonLifeState]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[PersonLifeState](
	[PersonRegistrationId] [uniqueidentifier] NOT NULL,
	[LifeStatusCodeTypeId] [int] NOT NULL,
	[EffectId] [uniqueidentifier] NULL,
 CONSTRAINT [PK_PersonLifeState] PRIMARY KEY CLUSTERED 
(
	[PersonRegistrationId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PersonCivilState]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[PersonCivilState](
	[PersonRegistrationId] [uniqueidentifier] NOT NULL,
	[CivilStatusCodeTypeId] [int] NOT NULL,
	[EffectId] [uniqueidentifier] NULL,
 CONSTRAINT [PK_PersonCivilState] PRIMARY KEY CLUSTERED 
(
	[PersonRegistrationId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[HealthInformation]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[HealthInformation](
	[PersonAttributesId] [uniqueidentifier] NOT NULL,
	[PhysicianName] [varchar](200) NULL,
	[PhysicianProviderNumber] [varchar](50) NULL,
	[HealthInsuranceGroupCode] [varchar](50) NULL,
	[EffectId] [uniqueidentifier] NULL,
 CONSTRAINT [PK_HealthInformation] PRIMARY KEY CLUSTERED 
(
	[PersonAttributesId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GreenlandicAddress]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[GreenlandicAddress](
	[AddressId] [uniqueidentifier] NOT NULL,
	[GreenlandBuildingIdentifierField] [varchar](4) NULL,
 CONSTRAINT [PK_GreenlandicAddress] PRIMARY KEY CLUSTERED 
(
	[AddressId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DanishAddress]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[DanishAddress](
	[AddressId] [uniqueidentifier] NOT NULL,
	[AddressPointId] [uniqueidentifier] NULL,
	[SocialDistrict] [varchar](50) NULL,
	[SchoolDistrict] [varchar](50) NULL,
	[PostDistrict] [varchar](50) NULL,
	[ParishDistrict] [varchar](50) NULL,
	[ConstituencyDistrict] [varchar](50) NULL,
	[PoliceDistrict] [varchar](50) NULL,
	[PostOfficeBoxIdentifier] [varchar](50) NULL,
 CONSTRAINT [PK_DanishAddress] PRIMARY KEY CLUSTERED 
(
	[AddressId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CprData]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[CprData](
	[PersonAttributesId] [uniqueidentifier] NOT NULL,
	[CprNumber] [varchar](10) NULL,
	[CprNumberValidity] [bit] NOT NULL,
	[NationalityCountryRefId] [uniqueidentifier] NULL,
	[NameAndAddressProtectionIndicator] [bit] NOT NULL,
	[TelephoneNumberProtection] [bit] NOT NULL,
	[ResearchProtection] [bit] NOT NULL,
	[AddressId] [uniqueidentifier] NULL,
	[AddressNote] [varchar](50) NULL,
	[ChurchMember] [bit] NOT NULL,
 CONSTRAINT [PK_CprData] PRIMARY KEY CLUSTERED 
(
	[PersonAttributesId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ForeignCitizenData]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[ForeignCitizenData](
	[PersonAttributesId] [uniqueidentifier] NOT NULL,
	[PersonIdentifier] [varchar](50) NULL,
	[CivilRegistrationReplacementIdentifier] [varchar](50) NULL,
	[BirthCountryRefId] [uniqueidentifier] NULL,
 CONSTRAINT [PK_ForeignCitizenData] PRIMARY KEY CLUSTERED 
(
	[PersonAttributesId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PersonProperties]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[PersonProperties](
	[PersonAttributesId] [uniqueidentifier] NOT NULL,
	[NickName] [varchar](50) NULL,
	[NameNoteText] [varchar](50) NULL,
	[AddressingName] [varchar](50) NULL,
	[GenderId] [int] NOT NULL,
	[BirthDate] [datetime] NOT NULL,
	[BirthPlace] [varchar](132) NULL,
	[BirthRegistrationAuthority] [varchar](60) NULL,
	[EffectId] [uniqueidentifier] NULL,
	[ContactChannelId] [uniqueidentifier] NULL,
	[NextOfKinContactChannelId] [uniqueidentifier] NULL,
	[OtherAddressId] [uniqueidentifier] NULL,
 CONSTRAINT [PK_PersonAttributes] PRIMARY KEY CLUSTERED 
(
	[PersonAttributesId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PersonName]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[PersonName](
	[PersonAttributesId] [uniqueidentifier] NOT NULL,
	[FirstName] [varchar](50) NULL,
	[MiddleName] [varchar](50) NULL,
	[LastName] [varchar](50) NULL,
 CONSTRAINT [PK_PersonName] PRIMARY KEY CLUSTERED 
(
	[PersonAttributesId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ForeignCitizenCountry]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[ForeignCitizenCountry](
	[ForeignCitizenCountryId] [uniqueidentifier] NOT NULL,
	[PersonAttributesId] [uniqueidentifier] NOT NULL,
	[Ordinal] [int] NOT NULL,
	[IsNationality] [bit] NOT NULL,
	[CountryRefId] [uniqueidentifier] NOT NULL,
 CONSTRAINT [PK_ForeignCitizenCountry] PRIMARY KEY CLUSTERED 
(
	[ForeignCitizenCountryId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY],
 CONSTRAINT [IX_ForeignCitizenCountry_Unique] UNIQUE NONCLUSTERED 
(
	[PersonAttributesId] ASC,
	[IsNationality] ASC,
	[Ordinal] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[AddressPoint]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[AddressPoint](
	[AddressId] [uniqueidentifier] NOT NULL,
	[Identifier] [varchar](50) NULL,
 CONSTRAINT [PK_AddressPoint] PRIMARY KEY CLUSTERED 
(
	[AddressId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[AddressPointStatus]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[AddressPointStatus](
	[AddressId] [uniqueidentifier] NOT NULL,
	[RevisionDate] [datetime] NOT NULL,
	[ValidStartDate] [datetime] NULL,
	[ValidEndDate] [datetime] NULL,
	[AddressCoordinateQualityTypeCode] [char](1) NULL,
 CONSTRAINT [PK_AddressPointStatus] PRIMARY KEY CLUSTERED 
(
	[AddressId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GeographicPointLocation]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[GeographicPointLocation](
	[AddressId] [uniqueidentifier] NOT NULL,
	[CrsIdentifier] [varchar](50) NULL,
 CONSTRAINT [PK_GeographicPointLocation] PRIMARY KEY CLUSTERED 
(
	[AddressId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GeographicCoorditaneTuple]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[GeographicCoorditaneTuple](
	[AddressId] [uniqueidentifier] NOT NULL,
	[Easting] [decimal](18, 4) NOT NULL,
	[Northing] [decimal](18, 4) NOT NULL,
	[Height] [decimal](18, 4) NULL,
 CONSTRAINT [PK_GeographicCoorditaneTuple] PRIMARY KEY CLUSTERED 
(
	[AddressId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO

IF Not EXISTS (SELECT * FROM sys.default_constraints WHERE object_id = OBJECT_ID(N'[dbo].[DF_ActorRef_ActorRefId]') AND parent_object_id = OBJECT_ID(N'[dbo].[ActorRef]'))
Begin
IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF_ActorRef_ActorRefId]') AND type = 'D')
BEGIN
ALTER TABLE [dbo].[ActorRef] ADD  CONSTRAINT [DF_ActorRef_ActorRefId]  DEFAULT (newid()) FOR [ActorRefId]
END


End
GO

IF Not EXISTS (SELECT * FROM sys.default_constraints WHERE object_id = OBJECT_ID(N'[dbo].[DF_Address_AddressId]') AND parent_object_id = OBJECT_ID(N'[dbo].[Address]'))
Begin
IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF_Address_AddressId]') AND type = 'D')
BEGIN
ALTER TABLE [dbo].[Address] ADD  CONSTRAINT [DF_Address_AddressId]  DEFAULT (newid()) FOR [AddressId]
END


End
GO

IF Not EXISTS (SELECT * FROM sys.default_constraints WHERE object_id = OBJECT_ID(N'[dbo].[DF_Application_ApplicationId]') AND parent_object_id = OBJECT_ID(N'[dbo].[Application]'))
Begin
IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF_Application_ApplicationId]') AND type = 'D')
BEGIN
ALTER TABLE [dbo].[Application] ADD  CONSTRAINT [DF_Application_ApplicationId]  DEFAULT (newid()) FOR [ApplicationId]
END


End
GO

IF Not EXISTS (SELECT * FROM sys.default_constraints WHERE object_id = OBJECT_ID(N'[dbo].[DF_Application_IsApproved]') AND parent_object_id = OBJECT_ID(N'[dbo].[Application]'))
Begin
IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF_Application_IsApproved]') AND type = 'D')
BEGIN
ALTER TABLE [dbo].[Application] ADD  CONSTRAINT [DF_Application_IsApproved]  DEFAULT ((0)) FOR [IsApproved]
END


End
GO

IF Not EXISTS (SELECT * FROM sys.default_constraints WHERE object_id = OBJECT_ID(N'[dbo].[DF_ContactChannel_ContactChannelId]') AND parent_object_id = OBJECT_ID(N'[dbo].[ContactChannel]'))
Begin
IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF_ContactChannel_ContactChannelId]') AND type = 'D')
BEGIN
ALTER TABLE [dbo].[ContactChannel] ADD  CONSTRAINT [DF_ContactChannel_ContactChannelId]  DEFAULT (newid()) FOR [ContactChannelId]
END


End
GO

IF Not EXISTS (SELECT * FROM sys.default_constraints WHERE object_id = OBJECT_ID(N'[dbo].[DF_CountryRef_CountryRefId]') AND parent_object_id = OBJECT_ID(N'[dbo].[CountryRef]'))
Begin
IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF_CountryRef_CountryRefId]') AND type = 'D')
BEGIN
ALTER TABLE [dbo].[CountryRef] ADD  CONSTRAINT [DF_CountryRef_CountryRefId]  DEFAULT (newid()) FOR [CountryRefId]
END


End
GO

IF Not EXISTS (SELECT * FROM sys.default_constraints WHERE object_id = OBJECT_ID(N'[dbo].[DF_Extract_ExtractId]') AND parent_object_id = OBJECT_ID(N'[dbo].[Extract]'))
Begin
IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF_Extract_ExtractId]') AND type = 'D')
BEGIN
ALTER TABLE [dbo].[Extract] ADD  CONSTRAINT [DF_Extract_ExtractId]  DEFAULT (newid()) FOR [ExtractId]
END


End
GO

IF Not EXISTS (SELECT * FROM sys.default_constraints WHERE object_id = OBJECT_ID(N'[dbo].[DF_Extract_Ready]') AND parent_object_id = OBJECT_ID(N'[dbo].[Extract]'))
Begin
IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF_Extract_Ready]') AND type = 'D')
BEGIN
ALTER TABLE [dbo].[Extract] ADD  CONSTRAINT [DF_Extract_Ready]  DEFAULT ((0)) FOR [Ready]
END


End
GO

IF Not EXISTS (SELECT * FROM sys.default_constraints WHERE object_id = OBJECT_ID(N'[dbo].[DF__ExtractEr__Extra__14270015]') AND parent_object_id = OBJECT_ID(N'[dbo].[ExtractError]'))
Begin
IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF__ExtractEr__Extra__14270015]') AND type = 'D')
BEGIN
ALTER TABLE [dbo].[ExtractError] ADD  DEFAULT (newid()) FOR [ExtractErrorId]
END


End
GO

IF Not EXISTS (SELECT * FROM sys.default_constraints WHERE object_id = OBJECT_ID(N'[dbo].[DF_ExtractItem_ExtractItemId]') AND parent_object_id = OBJECT_ID(N'[dbo].[ExtractItem]'))
Begin
IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF_ExtractItem_ExtractItemId]') AND type = 'D')
BEGIN
ALTER TABLE [dbo].[ExtractItem] ADD  CONSTRAINT [DF_ExtractItem_ExtractItemId]  DEFAULT (newid()) FOR [ExtractItemId]
END


End
GO

IF Not EXISTS (SELECT * FROM sys.default_constraints WHERE object_id = OBJECT_ID(N'[dbo].[DF_ExtractPersonStaging_ExtractPersonStagingId]') AND parent_object_id = OBJECT_ID(N'[dbo].[ExtractPersonStaging]'))
Begin
IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF_ExtractPersonStaging_ExtractPersonStagingId]') AND type = 'D')
BEGIN
ALTER TABLE [dbo].[ExtractPersonStaging] ADD  CONSTRAINT [DF_ExtractPersonStaging_ExtractPersonStagingId]  DEFAULT (newid()) FOR [ExtractPersonStagingId]
END


End
GO

IF Not EXISTS (SELECT * FROM sys.default_constraints WHERE object_id = OBJECT_ID(N'[dbo].[DF_Person_UUID]') AND parent_object_id = OBJECT_ID(N'[dbo].[Person]'))
Begin
IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF_Person_UUID]') AND type = 'D')
BEGIN
ALTER TABLE [dbo].[Person] ADD  CONSTRAINT [DF_Person_UUID]  DEFAULT (newid()) FOR [UUID]
END


End
GO

IF Not EXISTS (SELECT * FROM sys.default_constraints WHERE object_id = OBJECT_ID(N'[dbo].[DF__PersonAtt__Perso__160F4887]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonAttributes]'))
Begin
IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF__PersonAtt__Perso__160F4887]') AND type = 'D')
BEGIN
ALTER TABLE [dbo].[PersonAttributes] ADD  DEFAULT (newid()) FOR [PersonRegistrationId]
END


End
GO

IF Not EXISTS (SELECT * FROM sys.default_constraints WHERE object_id = OBJECT_ID(N'[dbo].[DF_PersonRegistration_PersonRegistrationId]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonRegistration]'))
Begin
IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF_PersonRegistration_PersonRegistrationId]') AND type = 'D')
BEGIN
ALTER TABLE [dbo].[PersonRegistration] ADD  CONSTRAINT [DF_PersonRegistration_PersonRegistrationId]  DEFAULT (newid()) FOR [PersonRegistrationId]
END


End
GO

IF Not EXISTS (SELECT * FROM sys.default_constraints WHERE object_id = OBJECT_ID(N'[dbo].[DF_PersonRelationship_PersonRelationshipId]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonRelationship]'))
Begin
IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF_PersonRelationship_PersonRelationshipId]') AND type = 'D')
BEGIN
ALTER TABLE [dbo].[PersonRelationship] ADD  CONSTRAINT [DF_PersonRelationship_PersonRelationshipId]  DEFAULT (newid()) FOR [PersonRelationshipId]
END


End
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_AddressPoint_DanishAddress]') AND parent_object_id = OBJECT_ID(N'[dbo].[AddressPoint]'))
ALTER TABLE [dbo].[AddressPoint]  WITH CHECK ADD  CONSTRAINT [FK_AddressPoint_DanishAddress] FOREIGN KEY([AddressId])
REFERENCES [dbo].[DanishAddress] ([AddressId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_AddressPoint_DanishAddress]') AND parent_object_id = OBJECT_ID(N'[dbo].[AddressPoint]'))
ALTER TABLE [dbo].[AddressPoint] CHECK CONSTRAINT [FK_AddressPoint_DanishAddress]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_AddressPointStatus_AddressCoordinateQualityType]') AND parent_object_id = OBJECT_ID(N'[dbo].[AddressPointStatus]'))
ALTER TABLE [dbo].[AddressPointStatus]  WITH CHECK ADD  CONSTRAINT [FK_AddressPointStatus_AddressCoordinateQualityType] FOREIGN KEY([AddressCoordinateQualityTypeCode])
REFERENCES [dbo].[AddressCoordinateQualityType] ([Code])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_AddressPointStatus_AddressCoordinateQualityType]') AND parent_object_id = OBJECT_ID(N'[dbo].[AddressPointStatus]'))
ALTER TABLE [dbo].[AddressPointStatus] CHECK CONSTRAINT [FK_AddressPointStatus_AddressCoordinateQualityType]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_AddressPointStatus_AddressPoint]') AND parent_object_id = OBJECT_ID(N'[dbo].[AddressPointStatus]'))
ALTER TABLE [dbo].[AddressPointStatus]  WITH CHECK ADD  CONSTRAINT [FK_AddressPointStatus_AddressPoint] FOREIGN KEY([AddressId])
REFERENCES [dbo].[AddressPoint] ([AddressId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_AddressPointStatus_AddressPoint]') AND parent_object_id = OBJECT_ID(N'[dbo].[AddressPointStatus]'))
ALTER TABLE [dbo].[AddressPointStatus] CHECK CONSTRAINT [FK_AddressPointStatus_AddressPoint]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_ContactChannel_ContactChannelType]') AND parent_object_id = OBJECT_ID(N'[dbo].[ContactChannel]'))
ALTER TABLE [dbo].[ContactChannel]  WITH CHECK ADD  CONSTRAINT [FK_ContactChannel_ContactChannelType] FOREIGN KEY([ContactChannelTypeId])
REFERENCES [dbo].[ContactChannelType] ([ContactChannelTypeId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_ContactChannel_ContactChannelType]') AND parent_object_id = OBJECT_ID(N'[dbo].[ContactChannel]'))
ALTER TABLE [dbo].[ContactChannel] CHECK CONSTRAINT [FK_ContactChannel_ContactChannelType]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_CountryRef_CountrySchemeType]') AND parent_object_id = OBJECT_ID(N'[dbo].[CountryRef]'))
ALTER TABLE [dbo].[CountryRef]  WITH CHECK ADD  CONSTRAINT [FK_CountryRef_CountrySchemeType] FOREIGN KEY([CountrySchemeTypeId])
REFERENCES [dbo].[CountrySchemeType] ([Id])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_CountryRef_CountrySchemeType]') AND parent_object_id = OBJECT_ID(N'[dbo].[CountryRef]'))
ALTER TABLE [dbo].[CountryRef] CHECK CONSTRAINT [FK_CountryRef_CountrySchemeType]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_CprData_Address]') AND parent_object_id = OBJECT_ID(N'[dbo].[CprData]'))
ALTER TABLE [dbo].[CprData]  WITH CHECK ADD  CONSTRAINT [FK_CprData_Address] FOREIGN KEY([AddressId])
REFERENCES [dbo].[Address] ([AddressId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_CprData_Address]') AND parent_object_id = OBJECT_ID(N'[dbo].[CprData]'))
ALTER TABLE [dbo].[CprData] CHECK CONSTRAINT [FK_CprData_Address]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_CprData_CountryRef]') AND parent_object_id = OBJECT_ID(N'[dbo].[CprData]'))
ALTER TABLE [dbo].[CprData]  WITH CHECK ADD  CONSTRAINT [FK_CprData_CountryRef] FOREIGN KEY([NationalityCountryRefId])
REFERENCES [dbo].[CountryRef] ([CountryRefId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_CprData_CountryRef]') AND parent_object_id = OBJECT_ID(N'[dbo].[CprData]'))
ALTER TABLE [dbo].[CprData] CHECK CONSTRAINT [FK_CprData_CountryRef]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_CprData_PersonAttributes]') AND parent_object_id = OBJECT_ID(N'[dbo].[CprData]'))
ALTER TABLE [dbo].[CprData]  WITH CHECK ADD  CONSTRAINT [FK_CprData_PersonAttributes] FOREIGN KEY([PersonAttributesId])
REFERENCES [dbo].[PersonAttributes] ([PersonAttributesId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_CprData_PersonAttributes]') AND parent_object_id = OBJECT_ID(N'[dbo].[CprData]'))
ALTER TABLE [dbo].[CprData] CHECK CONSTRAINT [FK_CprData_PersonAttributes]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_DanishAddress_DenmarkAddress]') AND parent_object_id = OBJECT_ID(N'[dbo].[DanishAddress]'))
ALTER TABLE [dbo].[DanishAddress]  WITH CHECK ADD  CONSTRAINT [FK_DanishAddress_DenmarkAddress] FOREIGN KEY([AddressId])
REFERENCES [dbo].[DenmarkAddress] ([AddressId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_DanishAddress_DenmarkAddress]') AND parent_object_id = OBJECT_ID(N'[dbo].[DanishAddress]'))
ALTER TABLE [dbo].[DanishAddress] CHECK CONSTRAINT [FK_DanishAddress_DenmarkAddress]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_DenmarkAddress_Address]') AND parent_object_id = OBJECT_ID(N'[dbo].[DenmarkAddress]'))
ALTER TABLE [dbo].[DenmarkAddress]  WITH CHECK ADD  CONSTRAINT [FK_DenmarkAddress_Address] FOREIGN KEY([AddressId])
REFERENCES [dbo].[Address] ([AddressId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_DenmarkAddress_Address]') AND parent_object_id = OBJECT_ID(N'[dbo].[DenmarkAddress]'))
ALTER TABLE [dbo].[DenmarkAddress] CHECK CONSTRAINT [FK_DenmarkAddress_Address]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_DenmarkAddress_CountryRef]') AND parent_object_id = OBJECT_ID(N'[dbo].[DenmarkAddress]'))
ALTER TABLE [dbo].[DenmarkAddress]  WITH CHECK ADD  CONSTRAINT [FK_DenmarkAddress_CountryRef] FOREIGN KEY([CountryRefId])
REFERENCES [dbo].[CountryRef] ([CountryRefId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_DenmarkAddress_CountryRef]') AND parent_object_id = OBJECT_ID(N'[dbo].[DenmarkAddress]'))
ALTER TABLE [dbo].[DenmarkAddress] CHECK CONSTRAINT [FK_DenmarkAddress_CountryRef]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_Effect_ActorRef]') AND parent_object_id = OBJECT_ID(N'[dbo].[Effect]'))
ALTER TABLE [dbo].[Effect]  WITH CHECK ADD  CONSTRAINT [FK_Effect_ActorRef] FOREIGN KEY([ActorRefId])
REFERENCES [dbo].[ActorRef] ([ActorRefId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_Effect_ActorRef]') AND parent_object_id = OBJECT_ID(N'[dbo].[Effect]'))
ALTER TABLE [dbo].[Effect] CHECK CONSTRAINT [FK_Effect_ActorRef]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_ExtractError_Extract]') AND parent_object_id = OBJECT_ID(N'[dbo].[ExtractError]'))
ALTER TABLE [dbo].[ExtractError]  WITH CHECK ADD  CONSTRAINT [FK_ExtractError_Extract] FOREIGN KEY([ExtractId])
REFERENCES [dbo].[Extract] ([ExtractId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_ExtractError_Extract]') AND parent_object_id = OBJECT_ID(N'[dbo].[ExtractError]'))
ALTER TABLE [dbo].[ExtractError] CHECK CONSTRAINT [FK_ExtractError_Extract]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_ExtractItem_Extract]') AND parent_object_id = OBJECT_ID(N'[dbo].[ExtractItem]'))
ALTER TABLE [dbo].[ExtractItem]  WITH NOCHECK ADD  CONSTRAINT [FK_ExtractItem_Extract] FOREIGN KEY([ExtractId])
REFERENCES [dbo].[Extract] ([ExtractId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_ExtractItem_Extract]') AND parent_object_id = OBJECT_ID(N'[dbo].[ExtractItem]'))
ALTER TABLE [dbo].[ExtractItem] CHECK CONSTRAINT [FK_ExtractItem_Extract]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_ExtractPersonStaging_Extract]') AND parent_object_id = OBJECT_ID(N'[dbo].[ExtractPersonStaging]'))
ALTER TABLE [dbo].[ExtractPersonStaging]  WITH CHECK ADD  CONSTRAINT [FK_ExtractPersonStaging_Extract] FOREIGN KEY([ExtractId])
REFERENCES [dbo].[Extract] ([ExtractId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_ExtractPersonStaging_Extract]') AND parent_object_id = OBJECT_ID(N'[dbo].[ExtractPersonStaging]'))
ALTER TABLE [dbo].[ExtractPersonStaging] CHECK CONSTRAINT [FK_ExtractPersonStaging_Extract]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_ForeignAddress_Address]') AND parent_object_id = OBJECT_ID(N'[dbo].[ForeignAddress]'))
ALTER TABLE [dbo].[ForeignAddress]  WITH CHECK ADD  CONSTRAINT [FK_ForeignAddress_Address] FOREIGN KEY([AddressId])
REFERENCES [dbo].[Address] ([AddressId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_ForeignAddress_Address]') AND parent_object_id = OBJECT_ID(N'[dbo].[ForeignAddress]'))
ALTER TABLE [dbo].[ForeignAddress] CHECK CONSTRAINT [FK_ForeignAddress_Address]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_ForeignAddress_CountryRef]') AND parent_object_id = OBJECT_ID(N'[dbo].[ForeignAddress]'))
ALTER TABLE [dbo].[ForeignAddress]  WITH CHECK ADD  CONSTRAINT [FK_ForeignAddress_CountryRef] FOREIGN KEY([CountryRefId])
REFERENCES [dbo].[CountryRef] ([CountryRefId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_ForeignAddress_CountryRef]') AND parent_object_id = OBJECT_ID(N'[dbo].[ForeignAddress]'))
ALTER TABLE [dbo].[ForeignAddress] CHECK CONSTRAINT [FK_ForeignAddress_CountryRef]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_ForeignCitizenCountry_CountryRef]') AND parent_object_id = OBJECT_ID(N'[dbo].[ForeignCitizenCountry]'))
ALTER TABLE [dbo].[ForeignCitizenCountry]  WITH CHECK ADD  CONSTRAINT [FK_ForeignCitizenCountry_CountryRef] FOREIGN KEY([CountryRefId])
REFERENCES [dbo].[CountryRef] ([CountryRefId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_ForeignCitizenCountry_CountryRef]') AND parent_object_id = OBJECT_ID(N'[dbo].[ForeignCitizenCountry]'))
ALTER TABLE [dbo].[ForeignCitizenCountry] CHECK CONSTRAINT [FK_ForeignCitizenCountry_CountryRef]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_ForeignCitizenCountry_ForeignCitizenData]') AND parent_object_id = OBJECT_ID(N'[dbo].[ForeignCitizenCountry]'))
ALTER TABLE [dbo].[ForeignCitizenCountry]  WITH CHECK ADD  CONSTRAINT [FK_ForeignCitizenCountry_ForeignCitizenData] FOREIGN KEY([PersonAttributesId])
REFERENCES [dbo].[ForeignCitizenData] ([PersonAttributesId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_ForeignCitizenCountry_ForeignCitizenData]') AND parent_object_id = OBJECT_ID(N'[dbo].[ForeignCitizenCountry]'))
ALTER TABLE [dbo].[ForeignCitizenCountry] CHECK CONSTRAINT [FK_ForeignCitizenCountry_ForeignCitizenData]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_ForeignCitizenData_CountryRef]') AND parent_object_id = OBJECT_ID(N'[dbo].[ForeignCitizenData]'))
ALTER TABLE [dbo].[ForeignCitizenData]  WITH CHECK ADD  CONSTRAINT [FK_ForeignCitizenData_CountryRef] FOREIGN KEY([BirthCountryRefId])
REFERENCES [dbo].[CountryRef] ([CountryRefId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_ForeignCitizenData_CountryRef]') AND parent_object_id = OBJECT_ID(N'[dbo].[ForeignCitizenData]'))
ALTER TABLE [dbo].[ForeignCitizenData] CHECK CONSTRAINT [FK_ForeignCitizenData_CountryRef]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_ForeignCitizenData_PersonAttributes]') AND parent_object_id = OBJECT_ID(N'[dbo].[ForeignCitizenData]'))
ALTER TABLE [dbo].[ForeignCitizenData]  WITH CHECK ADD  CONSTRAINT [FK_ForeignCitizenData_PersonAttributes] FOREIGN KEY([PersonAttributesId])
REFERENCES [dbo].[PersonAttributes] ([PersonAttributesId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_ForeignCitizenData_PersonAttributes]') AND parent_object_id = OBJECT_ID(N'[dbo].[ForeignCitizenData]'))
ALTER TABLE [dbo].[ForeignCitizenData] CHECK CONSTRAINT [FK_ForeignCitizenData_PersonAttributes]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_GeographicCoorditaneTuple_GeographicPointLocation]') AND parent_object_id = OBJECT_ID(N'[dbo].[GeographicCoorditaneTuple]'))
ALTER TABLE [dbo].[GeographicCoorditaneTuple]  WITH CHECK ADD  CONSTRAINT [FK_GeographicCoorditaneTuple_GeographicPointLocation] FOREIGN KEY([AddressId])
REFERENCES [dbo].[GeographicPointLocation] ([AddressId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_GeographicCoorditaneTuple_GeographicPointLocation]') AND parent_object_id = OBJECT_ID(N'[dbo].[GeographicCoorditaneTuple]'))
ALTER TABLE [dbo].[GeographicCoorditaneTuple] CHECK CONSTRAINT [FK_GeographicCoorditaneTuple_GeographicPointLocation]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_GeographicPointLocation_AddressPoint]') AND parent_object_id = OBJECT_ID(N'[dbo].[GeographicPointLocation]'))
ALTER TABLE [dbo].[GeographicPointLocation]  WITH CHECK ADD  CONSTRAINT [FK_GeographicPointLocation_AddressPoint] FOREIGN KEY([AddressId])
REFERENCES [dbo].[AddressPoint] ([AddressId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_GeographicPointLocation_AddressPoint]') AND parent_object_id = OBJECT_ID(N'[dbo].[GeographicPointLocation]'))
ALTER TABLE [dbo].[GeographicPointLocation] CHECK CONSTRAINT [FK_GeographicPointLocation_AddressPoint]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_GreenlandicAddress_DenmarkAddress]') AND parent_object_id = OBJECT_ID(N'[dbo].[GreenlandicAddress]'))
ALTER TABLE [dbo].[GreenlandicAddress]  WITH CHECK ADD  CONSTRAINT [FK_GreenlandicAddress_DenmarkAddress] FOREIGN KEY([AddressId])
REFERENCES [dbo].[DenmarkAddress] ([AddressId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_GreenlandicAddress_DenmarkAddress]') AND parent_object_id = OBJECT_ID(N'[dbo].[GreenlandicAddress]'))
ALTER TABLE [dbo].[GreenlandicAddress] CHECK CONSTRAINT [FK_GreenlandicAddress_DenmarkAddress]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_HealthInformation_Effect]') AND parent_object_id = OBJECT_ID(N'[dbo].[HealthInformation]'))
ALTER TABLE [dbo].[HealthInformation]  WITH CHECK ADD  CONSTRAINT [FK_HealthInformation_Effect] FOREIGN KEY([EffectId])
REFERENCES [dbo].[Effect] ([EffectId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_HealthInformation_Effect]') AND parent_object_id = OBJECT_ID(N'[dbo].[HealthInformation]'))
ALTER TABLE [dbo].[HealthInformation] CHECK CONSTRAINT [FK_HealthInformation_Effect]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_HealthInformation_PersonAttributes]') AND parent_object_id = OBJECT_ID(N'[dbo].[HealthInformation]'))
ALTER TABLE [dbo].[HealthInformation]  WITH CHECK ADD  CONSTRAINT [FK_HealthInformation_PersonAttributes] FOREIGN KEY([PersonAttributesId])
REFERENCES [dbo].[PersonAttributes] ([PersonAttributesId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_HealthInformation_PersonAttributes]') AND parent_object_id = OBJECT_ID(N'[dbo].[HealthInformation]'))
ALTER TABLE [dbo].[HealthInformation] CHECK CONSTRAINT [FK_HealthInformation_PersonAttributes]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_LogEntry_Application]') AND parent_object_id = OBJECT_ID(N'[dbo].[LogEntry]'))
ALTER TABLE [dbo].[LogEntry]  WITH NOCHECK ADD  CONSTRAINT [FK_LogEntry_Application] FOREIGN KEY([ApplicationId])
REFERENCES [dbo].[Application] ([ApplicationId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_LogEntry_Application]') AND parent_object_id = OBJECT_ID(N'[dbo].[LogEntry]'))
ALTER TABLE [dbo].[LogEntry] NOCHECK CONSTRAINT [FK_LogEntry_Application]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_LogEntry_LogType]') AND parent_object_id = OBJECT_ID(N'[dbo].[LogEntry]'))
ALTER TABLE [dbo].[LogEntry]  WITH CHECK ADD  CONSTRAINT [FK_LogEntry_LogType] FOREIGN KEY([LogTypeId])
REFERENCES [dbo].[LogType] ([LogTypeId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_LogEntry_LogType]') AND parent_object_id = OBJECT_ID(N'[dbo].[LogEntry]'))
ALTER TABLE [dbo].[LogEntry] CHECK CONSTRAINT [FK_LogEntry_LogType]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonAttributes_Effect]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonAttributes]'))
ALTER TABLE [dbo].[PersonAttributes]  WITH CHECK ADD  CONSTRAINT [FK_PersonAttributes_Effect] FOREIGN KEY([EffectId])
REFERENCES [dbo].[Effect] ([EffectId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonAttributes_Effect]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonAttributes]'))
ALTER TABLE [dbo].[PersonAttributes] CHECK CONSTRAINT [FK_PersonAttributes_Effect]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonAttributes_PersonRegistration]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonAttributes]'))
ALTER TABLE [dbo].[PersonAttributes]  WITH CHECK ADD  CONSTRAINT [FK_PersonAttributes_PersonRegistration] FOREIGN KEY([PersonRegistrationId])
REFERENCES [dbo].[PersonRegistration] ([PersonRegistrationId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonAttributes_PersonRegistration]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonAttributes]'))
ALTER TABLE [dbo].[PersonAttributes] CHECK CONSTRAINT [FK_PersonAttributes_PersonRegistration]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonCivilState_CivilStatusCodeType]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonCivilState]'))
ALTER TABLE [dbo].[PersonCivilState]  WITH CHECK ADD  CONSTRAINT [FK_PersonCivilState_CivilStatusCodeType] FOREIGN KEY([CivilStatusCodeTypeId])
REFERENCES [dbo].[CivilStatusCodeType] ([CivilStatusCodeTypeId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonCivilState_CivilStatusCodeType]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonCivilState]'))
ALTER TABLE [dbo].[PersonCivilState] CHECK CONSTRAINT [FK_PersonCivilState_CivilStatusCodeType]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonCivilState_Effect]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonCivilState]'))
ALTER TABLE [dbo].[PersonCivilState]  WITH CHECK ADD  CONSTRAINT [FK_PersonCivilState_Effect] FOREIGN KEY([EffectId])
REFERENCES [dbo].[Effect] ([EffectId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonCivilState_Effect]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonCivilState]'))
ALTER TABLE [dbo].[PersonCivilState] CHECK CONSTRAINT [FK_PersonCivilState_Effect]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonCivilState_PersonState]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonCivilState]'))
ALTER TABLE [dbo].[PersonCivilState]  WITH CHECK ADD  CONSTRAINT [FK_PersonCivilState_PersonState] FOREIGN KEY([PersonRegistrationId])
REFERENCES [dbo].[PersonState] ([PersonRegistrationId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonCivilState_PersonState]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonCivilState]'))
ALTER TABLE [dbo].[PersonCivilState] CHECK CONSTRAINT [FK_PersonCivilState_PersonState]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonLifeState_Effect]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonLifeState]'))
ALTER TABLE [dbo].[PersonLifeState]  WITH CHECK ADD  CONSTRAINT [FK_PersonLifeState_Effect] FOREIGN KEY([EffectId])
REFERENCES [dbo].[Effect] ([EffectId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonLifeState_Effect]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonLifeState]'))
ALTER TABLE [dbo].[PersonLifeState] CHECK CONSTRAINT [FK_PersonLifeState_Effect]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonLifeState_LifeStatusCodeType1]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonLifeState]'))
ALTER TABLE [dbo].[PersonLifeState]  WITH CHECK ADD  CONSTRAINT [FK_PersonLifeState_LifeStatusCodeType1] FOREIGN KEY([LifeStatusCodeTypeId])
REFERENCES [dbo].[LifeStatusCodeType] ([LifeStatusCodeTypeId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonLifeState_LifeStatusCodeType1]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonLifeState]'))
ALTER TABLE [dbo].[PersonLifeState] CHECK CONSTRAINT [FK_PersonLifeState_LifeStatusCodeType1]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonLifeState_PersonState]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonLifeState]'))
ALTER TABLE [dbo].[PersonLifeState]  WITH CHECK ADD  CONSTRAINT [FK_PersonLifeState_PersonState] FOREIGN KEY([PersonRegistrationId])
REFERENCES [dbo].[PersonState] ([PersonRegistrationId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonLifeState_PersonState]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonLifeState]'))
ALTER TABLE [dbo].[PersonLifeState] CHECK CONSTRAINT [FK_PersonLifeState_PersonState]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonName_PersonProperties]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonName]'))
ALTER TABLE [dbo].[PersonName]  WITH CHECK ADD  CONSTRAINT [FK_PersonName_PersonProperties] FOREIGN KEY([PersonAttributesId])
REFERENCES [dbo].[PersonProperties] ([PersonAttributesId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonName_PersonProperties]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonName]'))
ALTER TABLE [dbo].[PersonName] CHECK CONSTRAINT [FK_PersonName_PersonProperties]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonAttributes_Address]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonProperties]'))
ALTER TABLE [dbo].[PersonProperties]  WITH CHECK ADD  CONSTRAINT [FK_PersonAttributes_Address] FOREIGN KEY([OtherAddressId])
REFERENCES [dbo].[Address] ([AddressId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonAttributes_Address]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonProperties]'))
ALTER TABLE [dbo].[PersonProperties] CHECK CONSTRAINT [FK_PersonAttributes_Address]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonAttributes_ContactChannel]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonProperties]'))
ALTER TABLE [dbo].[PersonProperties]  WITH CHECK ADD  CONSTRAINT [FK_PersonAttributes_ContactChannel] FOREIGN KEY([ContactChannelId])
REFERENCES [dbo].[ContactChannel] ([ContactChannelId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonAttributes_ContactChannel]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonProperties]'))
ALTER TABLE [dbo].[PersonProperties] CHECK CONSTRAINT [FK_PersonAttributes_ContactChannel]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonAttributes_ContactChannel1]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonProperties]'))
ALTER TABLE [dbo].[PersonProperties]  WITH CHECK ADD  CONSTRAINT [FK_PersonAttributes_ContactChannel1] FOREIGN KEY([NextOfKinContactChannelId])
REFERENCES [dbo].[ContactChannel] ([ContactChannelId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonAttributes_ContactChannel1]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonProperties]'))
ALTER TABLE [dbo].[PersonProperties] CHECK CONSTRAINT [FK_PersonAttributes_ContactChannel1]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonAttributes_Gender]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonProperties]'))
ALTER TABLE [dbo].[PersonProperties]  WITH CHECK ADD  CONSTRAINT [FK_PersonAttributes_Gender] FOREIGN KEY([GenderId])
REFERENCES [dbo].[Gender] ([GenderId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonAttributes_Gender]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonProperties]'))
ALTER TABLE [dbo].[PersonProperties] CHECK CONSTRAINT [FK_PersonAttributes_Gender]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonProperties_PersonAttributes]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonProperties]'))
ALTER TABLE [dbo].[PersonProperties]  WITH CHECK ADD  CONSTRAINT [FK_PersonProperties_PersonAttributes] FOREIGN KEY([PersonAttributesId])
REFERENCES [dbo].[PersonAttributes] ([PersonAttributesId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonProperties_PersonAttributes]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonProperties]'))
ALTER TABLE [dbo].[PersonProperties] CHECK CONSTRAINT [FK_PersonProperties_PersonAttributes]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonRegistration_ActorRef]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonRegistration]'))
ALTER TABLE [dbo].[PersonRegistration]  WITH CHECK ADD  CONSTRAINT [FK_PersonRegistration_ActorRef] FOREIGN KEY([ActorRefId])
REFERENCES [dbo].[ActorRef] ([ActorRefId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonRegistration_ActorRef]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonRegistration]'))
ALTER TABLE [dbo].[PersonRegistration] CHECK CONSTRAINT [FK_PersonRegistration_ActorRef]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonRegistration_LifecycleStatus]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonRegistration]'))
ALTER TABLE [dbo].[PersonRegistration]  WITH CHECK ADD  CONSTRAINT [FK_PersonRegistration_LifecycleStatus] FOREIGN KEY([LifecycleStatusId])
REFERENCES [dbo].[LifecycleStatus] ([LifecycleStatusId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonRegistration_LifecycleStatus]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonRegistration]'))
ALTER TABLE [dbo].[PersonRegistration] CHECK CONSTRAINT [FK_PersonRegistration_LifecycleStatus]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonRegistration_Person]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonRegistration]'))
ALTER TABLE [dbo].[PersonRegistration]  WITH CHECK ADD  CONSTRAINT [FK_PersonRegistration_Person] FOREIGN KEY([UUID])
REFERENCES [dbo].[Person] ([UUID])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonRegistration_Person]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonRegistration]'))
ALTER TABLE [dbo].[PersonRegistration] CHECK CONSTRAINT [FK_PersonRegistration_Person]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonRelationship_Effect]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonRelationship]'))
ALTER TABLE [dbo].[PersonRelationship]  WITH CHECK ADD  CONSTRAINT [FK_PersonRelationship_Effect] FOREIGN KEY([EffectId])
REFERENCES [dbo].[Effect] ([EffectId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonRelationship_Effect]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonRelationship]'))
ALTER TABLE [dbo].[PersonRelationship] CHECK CONSTRAINT [FK_PersonRelationship_Effect]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonRelationship_PersonRegistration]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonRelationship]'))
ALTER TABLE [dbo].[PersonRelationship]  WITH CHECK ADD  CONSTRAINT [FK_PersonRelationship_PersonRegistration] FOREIGN KEY([PersonRegistrationId])
REFERENCES [dbo].[PersonRegistration] ([PersonRegistrationId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonRelationship_PersonRegistration]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonRelationship]'))
ALTER TABLE [dbo].[PersonRelationship] CHECK CONSTRAINT [FK_PersonRelationship_PersonRegistration]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonRelationship_RelationshipType]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonRelationship]'))
ALTER TABLE [dbo].[PersonRelationship]  WITH CHECK ADD  CONSTRAINT [FK_PersonRelationship_RelationshipType] FOREIGN KEY([RelationshipTypeId])
REFERENCES [dbo].[RelationshipType] ([RelationshipTypeId])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonRelationship_RelationshipType]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonRelationship]'))
ALTER TABLE [dbo].[PersonRelationship] CHECK CONSTRAINT [FK_PersonRelationship_RelationshipType]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonStates_PersonRegistration]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonState]'))
ALTER TABLE [dbo].[PersonState]  WITH CHECK ADD  CONSTRAINT [FK_PersonStates_PersonRegistration] FOREIGN KEY([PersonRegistrationId])
REFERENCES [dbo].[PersonRegistration] ([PersonRegistrationId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_PersonStates_PersonRegistration]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonState]'))
ALTER TABLE [dbo].[PersonState] CHECK CONSTRAINT [FK_PersonStates_PersonRegistration]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_UnknownCitizenData_PersonAttributes]') AND parent_object_id = OBJECT_ID(N'[dbo].[UnknownCitizenData]'))
ALTER TABLE [dbo].[UnknownCitizenData]  WITH CHECK ADD  CONSTRAINT [FK_UnknownCitizenData_PersonAttributes] FOREIGN KEY([PersonAttributesId])
REFERENCES [dbo].[PersonAttributes] ([PersonAttributesId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_UnknownCitizenData_PersonAttributes]') AND parent_object_id = OBJECT_ID(N'[dbo].[UnknownCitizenData]'))
ALTER TABLE [dbo].[UnknownCitizenData] CHECK CONSTRAINT [FK_UnknownCitizenData_PersonAttributes]
GO
