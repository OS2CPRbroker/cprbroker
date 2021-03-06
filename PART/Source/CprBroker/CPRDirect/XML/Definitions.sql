
    IF EXISTS (SELECT * FROM sys.views where name = 'PersonInformationView')
      DROP VIEW PersonInformationView;
    GO
    
    CREATE VIEW PersonInformationView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 10) AS CurrentCprNumber, SUBSTRING(Contents, 24, 2) AS Status, SUBSTRING(Contents, 26, 12) AS StatusStartDate, SUBSTRING(Contents, 38, 1) AS StatusDateUncertainty, SUBSTRING(Contents, 39, 1) AS Gender, SUBSTRING(Contents, 40, 10) AS Birthdate, SUBSTRING(Contents, 50, 1) AS BirthdateUncertainty, SUBSTRING(Contents, 51, 10) AS PersonStartDate, SUBSTRING(Contents, 61, 1) AS PersonStartDateUncertainty, SUBSTRING(Contents, 62, 10) AS PersonEndDate, SUBSTRING(Contents, 72, 1) AS PersonEndDateUncertainty, SUBSTRING(Contents, 73, 34) AS Job,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '001';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'CurrentAddressInformationView')
      DROP VIEW CurrentAddressInformationView;
    GO
    
    CREATE VIEW CurrentAddressInformationView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 4) AS MunicipalityCode, SUBSTRING(Contents, 18, 4) AS StreetCode, SUBSTRING(Contents, 22, 4) AS HouseNumber, SUBSTRING(Contents, 26, 2) AS Floor, SUBSTRING(Contents, 28, 4) AS Door, SUBSTRING(Contents, 32, 4) AS BuildingNumber, SUBSTRING(Contents, 36, 34) AS CareOfName, SUBSTRING(Contents, 70, 12) AS RelocationDate, SUBSTRING(Contents, 82, 1) AS RelocationDateUncertainty, SUBSTRING(Contents, 83, 12) AS MunicipalityArrivalDate, SUBSTRING(Contents, 95, 1) AS MunicipalityArrivalDateUncertainty, SUBSTRING(Contents, 96, 4) AS LeavingMunicipalityCode, SUBSTRING(Contents, 100, 12) AS LeavingMunicipalityDepartureDate, SUBSTRING(Contents, 112, 1) AS LeavingMunicipalityDepartureDateUncertainty, SUBSTRING(Contents, 113, 4) AS HomeAuthority, SUBSTRING(Contents, 117, 34) AS SupplementaryAddress1, SUBSTRING(Contents, 151, 34) AS SupplementaryAddress2, SUBSTRING(Contents, 185, 34) AS SupplementaryAddress3, SUBSTRING(Contents, 219, 34) AS SupplementaryAddress4, SUBSTRING(Contents, 253, 34) AS SupplementaryAddress5, SUBSTRING(Contents, 287, 10) AS StartDate, SUBSTRING(Contents, 297, 10) AS EndDate,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '002';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'ClearWrittenAddressView')
      DROP VIEW ClearWrittenAddressView;
    GO
    
    CREATE VIEW ClearWrittenAddressView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 34) AS AddressingName, SUBSTRING(Contents, 48, 34) AS CareOfName, SUBSTRING(Contents, 82, 34) AS Location, SUBSTRING(Contents, 116, 34) AS LabelledAddress, SUBSTRING(Contents, 150, 34) AS CityName, SUBSTRING(Contents, 184, 4) AS PostCode, SUBSTRING(Contents, 188, 20) AS PostDistrictText, SUBSTRING(Contents, 208, 4) AS MunicipalityCode, SUBSTRING(Contents, 212, 4) AS StreetCode, SUBSTRING(Contents, 216, 4) AS HouseNumber, SUBSTRING(Contents, 220, 2) AS Floor, SUBSTRING(Contents, 222, 4) AS Door, SUBSTRING(Contents, 226, 4) AS BuildingNumber, SUBSTRING(Contents, 230, 20) AS StreetAddressingName,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '003';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'ProtectionView')
      DROP VIEW ProtectionView;
    GO
    
    CREATE VIEW ProtectionView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 4) AS ProtectionType_, SUBSTRING(Contents, 18, 10) AS StartDate, SUBSTRING(Contents, 28, 10) AS EndDate,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '004';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'CurrentDepartureDataView')
      DROP VIEW CurrentDepartureDataView;
    GO
    
    CREATE VIEW CurrentDepartureDataView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 4) AS ExitCountryCode, SUBSTRING(Contents, 18, 12) AS ExitDate, SUBSTRING(Contents, 30, 1) AS ExitDateUncertainty, SUBSTRING(Contents, 31, 34) AS ForeignAddress1, SUBSTRING(Contents, 65, 34) AS ForeignAddress2, SUBSTRING(Contents, 99, 34) AS ForeignAddress3, SUBSTRING(Contents, 133, 34) AS ForeignAddress4, SUBSTRING(Contents, 167, 34) AS ForeignAddress5,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '005';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'ContactAddressView')
      DROP VIEW ContactAddressView;
    GO
    
    CREATE VIEW ContactAddressView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 34) AS Line1, SUBSTRING(Contents, 48, 34) AS Line2, SUBSTRING(Contents, 82, 34) AS Line3, SUBSTRING(Contents, 116, 34) AS Line4, SUBSTRING(Contents, 150, 34) AS Line5, SUBSTRING(Contents, 184, 10) AS StartDate, SUBSTRING(Contents, 194, 10) AS EndDate,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '006';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'CurrentDisappearanceInformationView')
      DROP VIEW CurrentDisappearanceInformationView;
    GO
    
    CREATE VIEW CurrentDisappearanceInformationView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 12) AS DisappearanceDate, SUBSTRING(Contents, 26, 1) AS DisappearanceDateUncertainty,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '007';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'CurrentNameInformationView')
      DROP VIEW CurrentNameInformationView;
    GO
    
    CREATE VIEW CurrentNameInformationView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 50) AS FirstName_s, SUBSTRING(Contents, 64, 1) AS FirstNameMarker, SUBSTRING(Contents, 65, 40) AS MiddleName, SUBSTRING(Contents, 105, 1) AS MiddleNameMarker, SUBSTRING(Contents, 106, 40) AS LastName, SUBSTRING(Contents, 146, 1) AS LastNameMarker, SUBSTRING(Contents, 147, 12) AS NameStartDate, SUBSTRING(Contents, 159, 1) AS NameStartDateUncertainty, SUBSTRING(Contents, 160, 34) AS AddressingName,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '008';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'BirthRegistrationInformationView')
      DROP VIEW BirthRegistrationInformationView;
    GO
    
    CREATE VIEW BirthRegistrationInformationView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 4) AS BirthRegistrationAuthorityCode, SUBSTRING(Contents, 18, 20) AS AdditionalBirthRegistrationText,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '009';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'CurrentCitizenshipView')
      DROP VIEW CurrentCitizenshipView;
    GO
    
    CREATE VIEW CurrentCitizenshipView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 4) AS CountryCode, SUBSTRING(Contents, 18, 12) AS CitizenshipStartDate, SUBSTRING(Contents, 30, 1) AS CitizenshipStartDateUncertainty,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '010';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'ChurchInformationView')
      DROP VIEW ChurchInformationView;
    GO
    
    CREATE VIEW ChurchInformationView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 1) AS ChurchRelationship, SUBSTRING(Contents, 15, 10) AS StartDate, SUBSTRING(Contents, 25, 1) AS StartDateUncertainty,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '011';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'CurrentCivilStatusView')
      DROP VIEW CurrentCivilStatusView;
    GO
    
    CREATE VIEW CurrentCivilStatusView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 1) AS CivilStatusCode, SUBSTRING(Contents, 15, 10) AS SpousePNR, SUBSTRING(Contents, 25, 10) AS SpouseBirthDate, SUBSTRING(Contents, 35, 1) AS SpouseBirthDateUncertainty, SUBSTRING(Contents, 36, 34) AS SpouseName, SUBSTRING(Contents, 70, 1) AS SpouseNameMarker, SUBSTRING(Contents, 71, 12) AS CivilStatusStartDate, SUBSTRING(Contents, 83, 1) AS CivilStatusStartDateUncertainty, SUBSTRING(Contents, 84, 12) AS ReferenceToAnySeparation,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '012';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'CurrentSeparationView')
      DROP VIEW CurrentSeparationView;
    GO
    
    CREATE VIEW CurrentSeparationView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 12) AS ReferenceToAnyMaritalStatus, SUBSTRING(Contents, 26, 10) AS SeparationStartDate, SUBSTRING(Contents, 36, 1) AS SeparationStartDateUncertainty,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '013';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'ChildView')
      DROP VIEW ChildView;
    GO
    
    CREATE VIEW ChildView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 10) AS ChildPNR,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '014';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'ParentsInformationView')
      DROP VIEW ParentsInformationView;
    GO
    
    CREATE VIEW ParentsInformationView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 10) AS MotherDate, SUBSTRING(Contents, 24, 1) AS MotherDateUncertainty, SUBSTRING(Contents, 25, 10) AS MotherPNR, SUBSTRING(Contents, 35, 10) AS MotherBirthDate, SUBSTRING(Contents, 45, 1) AS MotherBirthDateUncertainty, SUBSTRING(Contents, 46, 34) AS MotherName, SUBSTRING(Contents, 80, 1) AS MotherNameUncertainty, SUBSTRING(Contents, 81, 10) AS FatherDate, SUBSTRING(Contents, 91, 1) AS FatherDateUncertainty, SUBSTRING(Contents, 92, 10) AS FatherPNR, SUBSTRING(Contents, 102, 10) AS FatherBirthDate, SUBSTRING(Contents, 112, 1) AS FatherBirthDateUncertainty, SUBSTRING(Contents, 113, 34) AS FatherName, SUBSTRING(Contents, 147, 1) AS FatherNameUncertainty,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '015';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'ParentalAuthorityView')
      DROP VIEW ParentalAuthorityView;
    GO
    
    CREATE VIEW ParentalAuthorityView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 4) AS RelationshipType, SUBSTRING(Contents, 18, 10) AS CustodyStartDate, SUBSTRING(Contents, 28, 1) AS CustodyStartDateUncertainty, SUBSTRING(Contents, 29, 10) AS CustodyEndDate, SUBSTRING(Contents, 49, 10) AS RelationPNRStartDate,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '016';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'DisempowermentView')
      DROP VIEW DisempowermentView;
    GO
    
    CREATE VIEW DisempowermentView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 10) AS DisempowermentStartDate, SUBSTRING(Contents, 24, 1) AS DisempowermentStartDateUncertainty, SUBSTRING(Contents, 25, 10) AS DisempowermentEndDate, SUBSTRING(Contents, 35, 4) AS GuardianRelationType, SUBSTRING(Contents, 49, 10) AS RelationPNRStartDate, SUBSTRING(Contents, 59, 34) AS GuardianName, SUBSTRING(Contents, 93, 10) AS GuardianAddressStartDate, SUBSTRING(Contents, 103, 34) AS RelationText1, SUBSTRING(Contents, 137, 34) AS RelationText2, SUBSTRING(Contents, 171, 34) AS RelationText3, SUBSTRING(Contents, 205, 34) AS RelationText4, SUBSTRING(Contents, 239, 34) AS RelationText5,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '017';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'MunicipalConditionsView')
      DROP VIEW MunicipalConditionsView;
    GO
    
    CREATE VIEW MunicipalConditionsView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 1) AS MunicipalConditionType, SUBSTRING(Contents, 15, 5) AS MunicipalConditionCode, SUBSTRING(Contents, 20, 10) AS MunicipalConditionStartDate, SUBSTRING(Contents, 30, 1) AS MunicipalConditionStartDateUncertaintyMarker, SUBSTRING(Contents, 31, 30) AS MunicipalConditionComment,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '018';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'NotesView')
      DROP VIEW NotesView;
    GO
    
    CREATE VIEW NotesView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 2) AS NoteNumber, SUBSTRING(Contents, 16, 40) AS NoteText, SUBSTRING(Contents, 56, 10) AS StartDate, SUBSTRING(Contents, 66, 10) AS EndDate,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '019';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'ElectionInformationView')
      DROP VIEW ElectionInformationView;
    GO
    
    CREATE VIEW ElectionInformationView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 4) AS ElectionCode, SUBSTRING(Contents, 18, 10) AS VotingDate, SUBSTRING(Contents, 28, 10) AS ElectionInfoStartDate, SUBSTRING(Contents, 1, 3) AS ElectionInfoDeletionDate,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '020';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'RelocationOrderView')
      DROP VIEW RelocationOrderView;
    GO
    
    CREATE VIEW RelocationOrderView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '021';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'HistoricalPNRView')
      DROP VIEW HistoricalPNRView;
    GO
    
    CREATE VIEW HistoricalPNRView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 10) AS OldPNR, SUBSTRING(Contents, 24, 10) AS OldPNRStartDate, SUBSTRING(Contents, 34, 1) AS OldPNRStartDateUncertainty, SUBSTRING(Contents, 35, 10) AS OldPNREndDate, SUBSTRING(Contents, 45, 1) AS OldPNREndDateUncertainty,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '022';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'HistoricalAddressView')
      DROP VIEW HistoricalAddressView;
    GO
    
    CREATE VIEW HistoricalAddressView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 1) AS CorrectionMarker, SUBSTRING(Contents, 15, 4) AS MunicipalityCode, SUBSTRING(Contents, 19, 4) AS StreetCode, SUBSTRING(Contents, 23, 4) AS HouseNumber, SUBSTRING(Contents, 27, 2) AS Floor, SUBSTRING(Contents, 29, 4) AS Door, SUBSTRING(Contents, 33, 4) AS BuildingNumber, SUBSTRING(Contents, 37, 34) AS CareOfName, SUBSTRING(Contents, 71, 12) AS RelocationDate, SUBSTRING(Contents, 83, 1) AS RelocationDateUncertainty, SUBSTRING(Contents, 84, 12) AS LeavingDate, SUBSTRING(Contents, 96, 1) AS LeavingDateUncertainty,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '023';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'HistoricalDepartureView')
      DROP VIEW HistoricalDepartureView;
    GO
    
    CREATE VIEW HistoricalDepartureView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 1) AS CorrectionMarker, SUBSTRING(Contents, 15, 4) AS ExitCountryCode, SUBSTRING(Contents, 19, 12) AS ExitDate, SUBSTRING(Contents, 31, 1) AS ExitDateUncertainty, SUBSTRING(Contents, 32, 4) AS EntryCountryCode, SUBSTRING(Contents, 36, 12) AS EntryDate, SUBSTRING(Contents, 48, 1) AS EntryDateUncertainty, SUBSTRING(Contents, 49, 34) AS ForeignAddress1, SUBSTRING(Contents, 83, 34) AS ForeignAddress2, SUBSTRING(Contents, 117, 34) AS ForeignAddress3, SUBSTRING(Contents, 151, 34) AS ForeignAddress4, SUBSTRING(Contents, 185, 34) AS ForeignAddress5,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '024';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'HistoricalDisappearanceView')
      DROP VIEW HistoricalDisappearanceView;
    GO
    
    CREATE VIEW HistoricalDisappearanceView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 1) AS CorrectionMarker, SUBSTRING(Contents, 15, 12) AS DisappearanceDate, SUBSTRING(Contents, 27, 1) AS DisappearanceDateUncertainty, SUBSTRING(Contents, 28, 12) AS RetrievalDate, SUBSTRING(Contents, 40, 1) AS RetrievalDateUncertainty,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '025';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'HistoricalNameView')
      DROP VIEW HistoricalNameView;
    GO
    
    CREATE VIEW HistoricalNameView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 1) AS CorrectionMarker, SUBSTRING(Contents, 15, 50) AS FirstName_s, SUBSTRING(Contents, 65, 1) AS FirstNameMarker, SUBSTRING(Contents, 66, 40) AS MiddleName, SUBSTRING(Contents, 106, 1) AS MiddleNameMarker, SUBSTRING(Contents, 107, 40) AS LastName, SUBSTRING(Contents, 147, 1) AS LastNameMarker, SUBSTRING(Contents, 148, 12) AS NameStartDate, SUBSTRING(Contents, 160, 1) AS NameStartDateUncertainty, SUBSTRING(Contents, 161, 12) AS NameEndDate, SUBSTRING(Contents, 173, 1) AS NameEndDateUncertainty,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '026';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'HistoricalCitizenshipView')
      DROP VIEW HistoricalCitizenshipView;
    GO
    
    CREATE VIEW HistoricalCitizenshipView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 1) AS CorrectionMarker, SUBSTRING(Contents, 15, 4) AS CountryCode, SUBSTRING(Contents, 19, 12) AS CitizenshipStartDate, SUBSTRING(Contents, 31, 1) AS CitizenshipStartDateUncertainty, SUBSTRING(Contents, 32, 12) AS CitizenshipEndDate, SUBSTRING(Contents, 44, 1) AS CitizenshipEndDateUncertainty,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '027';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'HistoricalChurchInformationView')
      DROP VIEW HistoricalChurchInformationView;
    GO
    
    CREATE VIEW HistoricalChurchInformationView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 1) AS ChurchRelationship, SUBSTRING(Contents, 15, 10) AS StartDate, SUBSTRING(Contents, 25, 1) AS StartDateUncertainty, SUBSTRING(Contents, 26, 10) AS EndDate, SUBSTRING(Contents, 36, 1) AS EndDateUncertainty,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '028';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'HistoricalCivilStatusView')
      DROP VIEW HistoricalCivilStatusView;
    GO
    
    CREATE VIEW HistoricalCivilStatusView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 1) AS CorrectionMarker, SUBSTRING(Contents, 15, 1) AS CivilStatusCode, SUBSTRING(Contents, 16, 10) AS SpousePNR, SUBSTRING(Contents, 26, 10) AS SpouseBirthdate, SUBSTRING(Contents, 36, 1) AS SpouseBirthdateUncertainty, SUBSTRING(Contents, 37, 34) AS SpouseName, SUBSTRING(Contents, 71, 1) AS SpouseNameMarker, SUBSTRING(Contents, 72, 12) AS CivilStatusStartDate, SUBSTRING(Contents, 84, 1) AS CivilStatusStartDateUncertainty, SUBSTRING(Contents, 85, 12) AS CivilStatusEndDate, SUBSTRING(Contents, 97, 1) AS CivilStatusEndDateUncertainty, SUBSTRING(Contents, 98, 12) AS ReferenceToAnySeparation,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '029';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'HistoricalSeparationView')
      DROP VIEW HistoricalSeparationView;
    GO
    
    CREATE VIEW HistoricalSeparationView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 1) AS CorrectionMarker, SUBSTRING(Contents, 15, 12) AS ReferenceToAnyMaritalStatus, SUBSTRING(Contents, 27, 10) AS SeparationStartDate, SUBSTRING(Contents, 37, 1) AS SeparationStartDateUncertainty, SUBSTRING(Contents, 38, 10) AS SeparationEndDate, SUBSTRING(Contents, 48, 1) AS SeparationEndDateUncertainty,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '030';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'MotherWithClearWrittenAddressView')
      DROP VIEW MotherWithClearWrittenAddressView;
    GO
    
    CREATE VIEW MotherWithClearWrittenAddressView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 14, 10) AS MotherPNR,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '031';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'FatherWithClearWrittenAddressView')
      DROP VIEW FatherWithClearWrittenAddressView;
    GO
    
    CREATE VIEW FatherWithClearWrittenAddressView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 14, 10) AS FatherPNR,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '032';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'ChildWithClearWrittenAddressView')
      DROP VIEW ChildWithClearWrittenAddressView;
    GO
    
    CREATE VIEW ChildWithClearWrittenAddressView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 14, 10) AS MotherPNR,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '033';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'EventsView')
      DROP VIEW EventsView;
    GO
    
    CREATE VIEW EventsView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 12) AS CprUpdateDate, SUBSTRING(Contents, 26, 3) AS Event_, SUBSTRING(Contents, 29, 2) AS DerivedMark,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '099';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'ErrorRecordView')
      DROP VIEW ErrorRecordView;
    GO
    
    CREATE VIEW ErrorRecordView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 4, 10) AS ErrorField, SUBSTRING(Contents, 14, 75) AS Input, SUBSTRING(Contents, 89, 4) AS ErrorNumber, SUBSTRING(Contents, 93, 65) AS ErrorText,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '910';
    
    GO
  
    IF EXISTS (SELECT * FROM sys.views where name = 'SubscriptionDeletionReceiptView')
      DROP VIEW SubscriptionDeletionReceiptView;
    GO
    
    CREATE VIEW SubscriptionDeletionReceiptView
    AS SELECT
    e.ExtractId, e.ExtractDate, PNR, RelationPNR, RelationPNR2, SUBSTRING(Contents, 1, 3) AS RecordType, SUBSTRING(Contents, 14, 12) AS UpdateTime, SUBSTRING(Contents, 26, 3) AS Incident, SUBSTRING(Contents, 29, 15) AS KeyConstant,  Contents
    FROM ExtractItem ei
    INNER JOIN Extract e
    ON e.ExtractId = ei.ExtractId
    WHERE DataTypeCode= '997';
    
    GO
  