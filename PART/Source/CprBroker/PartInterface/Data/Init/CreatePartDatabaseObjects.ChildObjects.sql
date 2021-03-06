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

IF Not EXISTS (SELECT * FROM sys.default_constraints WHERE object_id = OBJECT_ID(N'[dbo].[DF_Address_AddressId]') AND parent_object_id = OBJECT_ID(N'[dbo].[Address]'))
Begin
IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF_Address_AddressId]') AND type = 'D')
BEGIN
ALTER TABLE [dbo].[Address] ADD  CONSTRAINT [DF_Address_AddressId]  DEFAULT (newid()) FOR [AddressId]
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

IF Not EXISTS (SELECT * FROM sys.default_constraints WHERE object_id = OBJECT_ID(N'[dbo].[DF__PersonAtt__Perso__160F4887]') AND parent_object_id = OBJECT_ID(N'[dbo].[PersonAttributes]'))
Begin
IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF__PersonAtt__Perso__160F4887]') AND type = 'D')
BEGIN
ALTER TABLE [dbo].[PersonAttributes] ADD  DEFAULT (newid()) FOR [PersonRegistrationId]
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
