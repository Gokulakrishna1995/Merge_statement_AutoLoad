/* Step 1 
 Index  need   to create  for  master table  one time process  */ 





 CREATE nONCLUSTERED INDEX ix_ATTOMID 
ON  Attom_Master_Table(ATTOMID) 

CREATE nONCLUSTERED INDEX ix_ParcelNumberRaw 
ON  Attom_Master_Table(ParcelNumberRaw) 

CREATE  nONCLUSTERED   INDEX ix_SitusStateCountyFIPS 
ON  Attom_Master_Table(SitusStateCountyFIPS) 

CREATE  nONCLUSTERED   INDEX ix_TaxYearAssessed 
ON  Attom_Master_Table(TaxYearAssessed)


/* Step 2 
 Index  need   to create  for  Source table which means  Dump  */ 





 CREATE CLUSTERED INDEX ix_ATTOMID 
ON  Attom_Master_Table(ATTOMID) 

CREATE nONCLUSTERED INDEX ix_ParcelNumberRaw 
ON  Attom_Master_Table(ParcelNumberRaw) 

CREATE  nonCLUSTERED   INDEX ix_FIPS 
ON  Attom_Master_Table(SitusStateCountyFIPS) 


CREATE  nonCLUSTERED   INDEX ix_TaxYearAssessed 
ON  Attom_Master_Table(TaxYearAssessed) 


 
(2,'Attom_oct_2024_2'),
(3,'Attom_oct_2024_1'),
(4,'Attom_Sept_2024_3'),
(5,'Attom_Sept_2024_2'),
(6,'Attom_Sept_2024_1')



/*main Query for Data Loading  MonthWise*/

DECLARE @tables TABLE (id int,
TableName NVARCHAR(255) 
 );
INSERT INTO @tables (id,TableName) VALUES
(1,'Attom_Dec_2024_1_Gethsi'),
(2,'Attom_Dec_2024_2_Gethsi'),
(3,'Attom_Dec_2024_3_Gethsi'),
(4,'Attom_Dec_2024_4_Gethsi');
 


-- Step 3: Declare variables for cursor
DECLARE @tableName NVARCHAR(255);
DECLARE @sql NVARCHAR(MAX);

-- Step 4: Declare the cursor for table names
DECLARE table_cursor CURSOR FOR 
SELECT tableName FROM @tables order  by  id  ;

-- Step 5: Open the cursor and fetch the first table name
OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @tableName;

-- Step 6: Loop through the table names
WHILE @@FETCH_STATUS = 0
BEGIN
 SET @sql = '
          MERGE INTO  Attom_7Counties_Taxroll2023C_Karthick AS target
USING ' + QUOTENAME(@tableName) + ' AS source
ON target.ATTOMID = source.ATTOMID and  
  target.SitusStateCountyFIPS = source.SitusStateCountyFIPS  and
  target.ParcelNumberRaw = source.ParcelNumberRaw and
   target.TaxYearAssessed = source.TaxYearAssessed 
WHEN MATCHED   THEN
    UPDATE SET 
target.SitusStateCode	=	source.SitusStateCode,
target.PropertyJurisdictionName	=	source.PropertyJurisdictionName,
target.CombinedStatisticalArea	=	source.CombinedStatisticalArea,
target.CBSAName	=	source.CBSAName,
target.CBSACode	=	source.CBSACode,
target.MSAName	=	source.MSAName,
target.MSACode	=	source.MSACode,
target.MetropolitanDivision	=	source.MetropolitanDivision,
target.MinorCivilDivisionName	=	source.MinorCivilDivisionName,
target.MinorCivilDivisionCode	=	source.MinorCivilDivisionCode,
target.NeighborhoodCode	=	source.NeighborhoodCode,
target.CensusFIPSPlaceCode	=	source.CensusFIPSPlaceCode,
target.CensusTract	=	source.CensusTract,
target.CensusBlockGroup	=	source.CensusBlockGroup,
target.CensusBlock	=	source.CensusBlock,
target.ParcelNumberFormatted	=	source.ParcelNumberFormatted,
target.ParcelNumberYearAdded	=	source.ParcelNumberYearAdded,
target.ParcelNumberAlternate	=	source.ParcelNumberAlternate,
target.ParcelMapBook	=	source.ParcelMapBook,
target.ParcelMapPage	=	source.ParcelMapPage,
target.ParcelNumberYearChange	=	source.ParcelNumberYearChange,
target.ParcelNumberPrevious	=	source.ParcelNumberPrevious,
target.ParcelAccountNumber	=	source.ParcelAccountNumber,
target.PropertyAddressFull	=	source.PropertyAddressFull,
target.PropertyAddressHouseNumber	=	source.PropertyAddressHouseNumber,
target.PropertyAddressStreetDirection	=	source.PropertyAddressStreetDirection,
target.PropertyAddressStreetName	=	source.PropertyAddressStreetName,
target.PropertyAddressStreetSuffix	=	source.PropertyAddressStreetSuffix,
target.PropertyAddressStreetPostDirection	=	source.PropertyAddressStreetPostDirection,
target.PropertyAddressUnitPrefix	=	source.PropertyAddressUnitPrefix,
target.PropertyAddressUnitValue	=	source.PropertyAddressUnitValue,
target.PropertyAddressCity	=	source.PropertyAddressCity,
target.PropertyAddressState	=	source.PropertyAddressState,
target.PropertyAddressZIP	=	source.PropertyAddressZIP,
target.PropertyAddressZIP4	=	source.PropertyAddressZIP4,
target.PropertyAddressCRRT	=	source.PropertyAddressCRRT,
target.PropertyAddressInfoPrivacy	=	source.PropertyAddressInfoPrivacy,
target.CongressionalDistrictHouse	=	source.CongressionalDistrictHouse,
target.PropertyLatitude	=	source.PropertyLatitude,
target.PropertyLongitude	=	source.PropertyLongitude,
target.GeoQuality	=	source.GeoQuality,
target.LegalDescription	=	source.LegalDescription,
target.LegalRange	=	source.LegalRange,
target.LegalTownship	=	source.LegalTownship,
target.LegalSection	=	source.LegalSection,
target.LegalQuarter	=	source.LegalQuarter,
target.LegalQuarterQuarter	=	source.LegalQuarterQuarter,
target.LegalSubdivision	=	source.LegalSubdivision,
target.LegalPhase	=	source.LegalPhase,
target.LegalTractNumber	=	source.LegalTractNumber,
target.LegalBlock1	=	source.LegalBlock1,
target.LegalBlock2	=	source.LegalBlock2,
target.LegalLotNumber1	=	source.LegalLotNumber1,
target.LegalLotNumber2	=	source.LegalLotNumber2,
target.LegalLotNumber3	=	source.LegalLotNumber3,
target.LegalUnit	=	source.LegalUnit,
target.PartyOwner1NameFull	=	source.PartyOwner1NameFull,
target.PartyOwner1NameFirst	=	source.PartyOwner1NameFirst,
target.PartyOwner1NameMiddle	=	source.PartyOwner1NameMiddle,
target.PartyOwner1NameLast	=	source.PartyOwner1NameLast,
target.PartyOwner1NameSuffix	=	source.PartyOwner1NameSuffix,
target.TrustDescription	=	source.TrustDescription,
target.CompanyFlag	=	source.CompanyFlag,
target.PartyOwner2NameFull	=	source.PartyOwner2NameFull,
target.PartyOwner2NameFirst	=	source.PartyOwner2NameFirst,
target.PartyOwner2NameMiddle	=	source.PartyOwner2NameMiddle,
target.PartyOwner2NameLast	=	source.PartyOwner2NameLast,
target.PartyOwner2NameSuffix	=	source.PartyOwner2NameSuffix,
target.OwnerTypeDescription1	=	source.OwnerTypeDescription1,
target.OwnershipVestingRelationCode	=	source.OwnershipVestingRelationCode,
target.PartyOwner3NameFull	=	source.PartyOwner3NameFull,
target.PartyOwner3NameFirst	=	source.PartyOwner3NameFirst,
target.PartyOwner3NameMiddle	=	source.PartyOwner3NameMiddle,
target.PartyOwner3NameLast	=	source.PartyOwner3NameLast,
target.PartyOwner3NameSuffix	=	source.PartyOwner3NameSuffix,
target.PartyOwner4NameFull	=	source.PartyOwner4NameFull,
target.PartyOwner4NameFirst	=	source.PartyOwner4NameFirst,
target.PartyOwner4NameMiddle	=	source.PartyOwner4NameMiddle,
target.PartyOwner4NameLast	=	source.PartyOwner4NameLast,
target.PartyOwner4NameSuffix	=	source.PartyOwner4NameSuffix,
target.OwnerTypeDescription2	=	source.OwnerTypeDescription2,
target.ContactOwnerMailingCounty	=	source.ContactOwnerMailingCounty,
target.ContactOwnerMailingFIPS	=	source.ContactOwnerMailingFIPS,
target.ContactOwnerMailAddressFull	=	source.ContactOwnerMailAddressFull,
target.ContactOwnerMailAddressHouseNumber	=	source.ContactOwnerMailAddressHouseNumber,
target.ContactOwnerMailAddressStreetDirection	=	source.ContactOwnerMailAddressStreetDirection,
target.ContactOwnerMailAddressStreetName	=	source.ContactOwnerMailAddressStreetName,
target.ContactOwnerMailAddressStreetSuffix	=	source.ContactOwnerMailAddressStreetSuffix,
target.ContactOwnerMailAddressStreetPostDirection	=	source.ContactOwnerMailAddressStreetPostDirection,
target.ContactOwnerMailAddressUnitPrefix	=	source.ContactOwnerMailAddressUnitPrefix,
target.ContactOwnerMailAddressUnit	=	source.ContactOwnerMailAddressUnit,
target.ContactOwnerMailAddressCity	=	source.ContactOwnerMailAddressCity,
target.ContactOwnerMailAddressState	=	source.ContactOwnerMailAddressState,
target.ContactOwnerMailAddressZIP	=	source.ContactOwnerMailAddressZIP,
target.ContactOwnerMailAddressZIP4	=	source.ContactOwnerMailAddressZIP4,
target.ContactOwnerMailAddressCRRT	=	source.ContactOwnerMailAddressCRRT,
target.ContactOwnerMailAddressInfoFormat	=	source.ContactOwnerMailAddressInfoFormat,
target.ContactOwnerMailInfoPrivacy	=	source.ContactOwnerMailInfoPrivacy,
target.StatusOwnerOccupiedFlag	=	source.StatusOwnerOccupiedFlag,
target.DeedOwner1NameFull	=	source.DeedOwner1NameFull,
target.DeedOwner1NameFirst	=	source.DeedOwner1NameFirst,
target.DeedOwner1NameMiddle	=	source.DeedOwner1NameMiddle,
target.DeedOwner1NameLast	=	source.DeedOwner1NameLast,
target.DeedOwner1NameSuffix	=	source.DeedOwner1NameSuffix,
target.DeedOwner2NameFull	=	source.DeedOwner2NameFull,
target.DeedOwner2NameFirst	=	source.DeedOwner2NameFirst,
target.DeedOwner2NameMiddle	=	source.DeedOwner2NameMiddle,
target.DeedOwner2NameLast	=	source.DeedOwner2NameLast,
target.DeedOwner2NameSuffix	=	source.DeedOwner2NameSuffix,
target.DeedOwner3NameFull	=	source.DeedOwner3NameFull,
target.DeedOwner3NameFirst	=	source.DeedOwner3NameFirst,
target.DeedOwner3NameMiddle	=	source.DeedOwner3NameMiddle,
target.DeedOwner3NameLast	=	source.DeedOwner3NameLast,
target.DeedOwner3NameSuffix	=	source.DeedOwner3NameSuffix,
target.DeedOwner4NameFull	=	source.DeedOwner4NameFull,
target.DeedOwner4NameFirst	=	source.DeedOwner4NameFirst,
target.DeedOwner4NameMiddle	=	source.DeedOwner4NameMiddle,
target.DeedOwner4NameLast	=	source.DeedOwner4NameLast,
target.DeedOwner4NameSuffix	=	source.DeedOwner4NameSuffix,
target.TaxAssessedValueTotal	=	source.TaxAssessedValueTotal,
target.TaxAssessedValueImprovements	=	source.TaxAssessedValueImprovements,
target.TaxAssessedValueLand	=	source.TaxAssessedValueLand,
target.TaxAssessedImprovementsPerc	=	source.TaxAssessedImprovementsPerc,
target.PreviousAssessedValue	=	source.PreviousAssessedValue,
target.TaxMarketValueYear	=	source.TaxMarketValueYear,
target.TaxMarketValueTotal	=	source.TaxMarketValueTotal,
target.TaxMarketValueImprovements	=	source.TaxMarketValueImprovements,
target.TaxMarketValueLand	=	source.TaxMarketValueLand,
target.TaxMarketImprovementsPerc	=	source.TaxMarketImprovementsPerc,
target.TaxFiscalYear	=	source.TaxFiscalYear,
target.TaxRateArea	=	source.TaxRateArea,
target.TaxBilledAmount	=	source.TaxBilledAmount,
target.TaxDelinquentYear	=	source.TaxDelinquentYear,
target.LastAssessorTaxRollUpdate	=	source.LastAssessorTaxRollUpdate,
target.AssrLastUpdated	=	source.AssrLastUpdated,
target.TaxExemptionHomeownerFlag	=	source.TaxExemptionHomeownerFlag,
target.TaxExemptionDisabledFlag	=	source.TaxExemptionDisabledFlag,
target.TaxExemptionSeniorFlag	=	source.TaxExemptionSeniorFlag,
target.TaxExemptionVeteranFlag	=	source.TaxExemptionVeteranFlag,
target.TaxExemptionWidowFlag	=	source.TaxExemptionWidowFlag,
target.TaxExemptionAdditional	=	source.TaxExemptionAdditional,
target.YearBuilt	=	source.YearBuilt,
target.YearBuiltEffective	=	source.YearBuiltEffective,
target.ZonedCodeLocal	=	source.ZonedCodeLocal,
target.PropertyUseMuni	=	source.PropertyUseMuni,
target.PropertyUseGroup	=	source.PropertyUseGroup,
target.PropertyUseStandardized	=	source.PropertyUseStandardized,
target.AssessorLastSaleDate	=	source.AssessorLastSaleDate,
target.AssessorLastSaleAmount	=	source.AssessorLastSaleAmount,
target.AssessorPriorSaleDate	=	source.AssessorPriorSaleDate,
target.AssessorPriorSaleAmount	=	source.AssessorPriorSaleAmount,
target.LastOwnershipTransferDate	=	source.LastOwnershipTransferDate,
target.LastOwnershipTransferDocumentNumber	=	source.LastOwnershipTransferDocumentNumber,
target.LastOwnershipTransferTransactionID	=	source.LastOwnershipTransferTransactionID,
target.DeedLastSaleDocumentBook	=	source.DeedLastSaleDocumentBook,
target.DeedLastSaleDocumentPage	=	source.DeedLastSaleDocumentPage,
target.DeedLastDocumentNumber	=	source.DeedLastDocumentNumber,
target.DeedLastSaleDate	=	source.DeedLastSaleDate,
target.DeedLastSalePrice	=	source.DeedLastSalePrice,
target.DeedLastSaleTransactionID	=	source.DeedLastSaleTransactionID,
target.AreaBuilding	=	source.AreaBuilding,
target.AreaBuildingDefinitionCode	=	source.AreaBuildingDefinitionCode,
target.AreaGross	=	source.AreaGross,
target.Area1stFloor	=	source.Area1stFloor,
target.Area2ndFloor	=	source.Area2ndFloor,
target.AreaUpperFloors	=	source.AreaUpperFloors,
target.AreaLotAcres	=	source.AreaLotAcres,
target.AreaLotSF	=	source.AreaLotSF,
target.AreaLotDepth	=	source.AreaLotDepth,
target.AreaLotWidth	=	source.AreaLotWidth,
target.RoomsAtticArea	=	source.RoomsAtticArea,
target.RoomsAtticFlag	=	source.RoomsAtticFlag,
target.RoomsBasementArea	=	source.RoomsBasementArea,
target.RoomsBasementAreaFinished	=	source.RoomsBasementAreaFinished,
target.RoomsBasementAreaUnfinished	=	source.RoomsBasementAreaUnfinished,
target.ParkingGarage	=	source.ParkingGarage,
target.ParkingGarageArea	=	source.ParkingGarageArea,
target.ParkingCarport	=	source.ParkingCarport,
target.ParkingCarportArea	=	source.ParkingCarportArea,
target.HVACCoolingDetail	=	source.HVACCoolingDetail,
target.HVACHeatingDetail	=	source.HVACHeatingDetail,
target.HVACHeatingFuel	=	source.HVACHeatingFuel,
target.UtilitiesSewageUsage	=	source.UtilitiesSewageUsage,
target.UtilitiesWaterSource	=	source.UtilitiesWaterSource,
target.UtilitiesMobileHomeHookupFlag	=	source.UtilitiesMobileHomeHookupFlag,
target.Foundation	=	source.Foundation,
target.Construction	=	source.Construction,
target.InteriorStructure	=	source.InteriorStructure,
target.PlumbingFixturesCount	=	source.PlumbingFixturesCount,
target.ConstructionFireResistanceClass	=	source.ConstructionFireResistanceClass,
target.SafetyFireSprinklersFlag	=	source.SafetyFireSprinklersFlag,
target.FlooringMaterialPrimary	=	source.FlooringMaterialPrimary,
target.BathCount	=	source.BathCount,
target.BathPartialCount	=	source.BathPartialCount,
target.BedroomsCount	=	source.BedroomsCount,
target.RoomsCount	=	source.RoomsCount,
target.StoriesCount	=	source.StoriesCount,
target.UnitsCount	=	source.UnitsCount,
target.RoomsBonusRoomFlag	=	source.RoomsBonusRoomFlag,
target.RoomsBreakfastNookFlag	=	source.RoomsBreakfastNookFlag,
target.RoomsCellarFlag	=	source.RoomsCellarFlag,
target.RoomsCellarWineFlag	=	source.RoomsCellarWineFlag,
target.RoomsExerciseFlag	=	source.RoomsExerciseFlag,
target.RoomsFamilyCode	=	source.RoomsFamilyCode,
target.RoomsGameFlag	=	source.RoomsGameFlag,
target.RoomsGreatFlag	=	source.RoomsGreatFlag,
target.RoomsHobbyFlag	=	source.RoomsHobbyFlag,
target.RoomsLaundryFlag	=	source.RoomsLaundryFlag,
target.RoomsMediaFlag	=	source.RoomsMediaFlag,
target.RoomsMudFlag	=	source.RoomsMudFlag,
target.RoomsOfficeArea	=	source.RoomsOfficeArea,
target.RoomsOfficeFlag	=	source.RoomsOfficeFlag,
target.RoomsSafeRoomFlag	=	source.RoomsSafeRoomFlag,
target.RoomsSittingFlag	=	source.RoomsSittingFlag,
target.RoomsStormShelter	=	source.RoomsStormShelter,
target.RoomsStudyFlag	=	source.RoomsStudyFlag,
target.RoomsSunroomFlag	=	source.RoomsSunroomFlag,
target.RoomsUtilityArea	=	source.RoomsUtilityArea,
target.RoomsUtilityCode	=	source.RoomsUtilityCode,
target.Fireplace	=	source.Fireplace,
target.FireplaceCount	=	source.FireplaceCount,
target.AccessabilityElevatorFlag	=	source.AccessabilityElevatorFlag,
target.AccessabilityHandicapFlag	=	source.AccessabilityHandicapFlag,
target.EscalatorFlag	=	source.EscalatorFlag,
target.CentralVacuumFlag	=	source.CentralVacuumFlag,
target.ContentIntercomFlag	=	source.ContentIntercomFlag,
target.ContentSoundSystemFlag	=	source.ContentSoundSystemFlag,
target.WetBarFlag	=	source.WetBarFlag,
target.SecurityAlarmFlag	=	source.SecurityAlarmFlag,
target.StructureStyle	=	source.StructureStyle,
target.Exterior1Code	=	source.Exterior1Code,
target.RoofMaterial	=	source.RoofMaterial,
target.RoofConstruction	=	source.RoofConstruction,
target.ContentStormShutterFlag	=	source.ContentStormShutterFlag,
target.ContentOverheadDoorFlag	=	source.ContentOverheadDoorFlag,
target.ViewDescription	=	source.ViewDescription,
target.PorchCode	=	source.PorchCode,
target.PorchArea	=	source.PorchArea,
target.PatioArea	=	source.PatioArea,
target.DeckFlag	=	source.DeckFlag,
target.DeckArea	=	source.DeckArea,
target.FeatureBalconyFlag	=	source.FeatureBalconyFlag,
target.BalconyArea	=	source.BalconyArea,
target.BreezewayFlag	=	source.BreezewayFlag,
target.ParkingRVParkingFlag	=	source.ParkingRVParkingFlag,
target.ParkingSpaceCount	=	source.ParkingSpaceCount,
target.DrivewayArea	=	source.DrivewayArea,
target.DrivewayMaterial	=	source.DrivewayMaterial,
target.Pool	=	source.Pool,
target.PoolArea	=	source.PoolArea,
target.ContentSaunaFlag	=	source.ContentSaunaFlag,
target.TopographyCode	=	source.TopographyCode,
target.FenceCode	=	source.FenceCode,
target.FenceArea	=	source.FenceArea,
target.CourtyardFlag	=	source.CourtyardFlag,
target.CourtyardArea	=	source.CourtyardArea,
target.ArborPergolaFlag	=	source.ArborPergolaFlag,
target.SprinklersFlag	=	source.SprinklersFlag,
target.GolfCourseGreenFlag	=	source.GolfCourseGreenFlag,
target.TennisCourtFlag	=	source.TennisCourtFlag,
target.SportsCourtFlag	=	source.SportsCourtFlag,
target.ArenaFlag	=	source.ArenaFlag,
target.WaterFeatureFlag	=	source.WaterFeatureFlag,
target.PondFlag	=	source.PondFlag,
target.BoatLiftFlag	=	source.BoatLiftFlag,
target.BuildingsCount	=	source.BuildingsCount,
target.BathHouseArea	=	source.BathHouseArea,
target.BathHouseFlag	=	source.BathHouseFlag,
target.BoatAccessFlag	=	source.BoatAccessFlag,
target.BoatHouseArea	=	source.BoatHouseArea,
target.BoatHouseFlag	=	source.BoatHouseFlag,
target.CabinArea	=	source.CabinArea,
target.CabinFlag	=	source.CabinFlag,
target.CanopyArea	=	source.CanopyArea,
target.CanopyFlag	=	source.CanopyFlag,
target.GazeboArea	=	source.GazeboArea,
target.GazeboFlag	=	source.GazeboFlag,
target.GraineryArea	=	source.GraineryArea,
target.GraineryFlag	=	source.GraineryFlag,
target.GreenHouseArea	=	source.GreenHouseArea,
target.GreenHouseFlag	=	source.GreenHouseFlag,
target.GuestHouseArea	=	source.GuestHouseArea,
target.GuestHouseFlag	=	source.GuestHouseFlag,
target.KennelArea	=	source.KennelArea,
target.KennelFlag	=	source.KennelFlag,
target.LeanToArea	=	source.LeanToArea,
target.LeanToFlag	=	source.LeanToFlag,
target.LoadingPlatformArea	=	source.LoadingPlatformArea,
target.LoadingPlatformFlag	=	source.LoadingPlatformFlag,
target.MilkHouseArea	=	source.MilkHouseArea,
target.MilkHouseFlag	=	source.MilkHouseFlag,
target.OutdoorKitchenFireplaceFlag	=	source.OutdoorKitchenFireplaceFlag,
target.PoolHouseArea	=	source.PoolHouseArea,
target.PoolHouseFlag	=	source.PoolHouseFlag,
target.PoultryHouseArea	=	source.PoultryHouseArea,
target.PoultryHouseFlag	=	source.PoultryHouseFlag,
target.QuonsetArea	=	source.QuonsetArea,
target.QuonsetFlag	=	source.QuonsetFlag,
target.ShedArea	=	source.ShedArea,
target.ShedCode	=	source.ShedCode,
target.SiloArea	=	source.SiloArea,
target.SiloFlag	=	source.SiloFlag,
target.StableArea	=	source.StableArea,
target.StableFlag	=	source.StableFlag,
target.StorageBuildingArea	=	source.StorageBuildingArea,
target.StorageBuildingFlag	=	source.StorageBuildingFlag,
target.UtilityBuildingArea	=	source.UtilityBuildingArea,
target.UtilityBuildingFlag	=	source.UtilityBuildingFlag,
target.PoleStructureArea	=	source.PoleStructureArea,
target.PoleStructureFlag	=	source.PoleStructureFlag,
target.CommunityRecRoomFlag	=	source.CommunityRecRoomFlag,
target.PublicationDate	=	source.PublicationDate,
target.ParcelShellRecord	=	source.ParcelShellRecord,
target.Sourcefilename	=	 ''' + QUOTENAME(@tableName) + '''  
WHEN NOT MATCHED BY TARGET  and source.TaxYearAssessed in (''2023'',''2024'',''2025'') 

 THEN
 INSERT (ATTOMID,SitusStateCode,SitusCounty,PropertyJurisdictionName,SitusStateCountyFIPS,CombinedStatisticalArea,
 CBSAName,CBSACode,MSAName,MSACode,MetropolitanDivision,MinorCivilDivisionName,MinorCivilDivisionCode,NeighborhoodCode,
 CensusFIPSPlaceCode,CensusTract,CensusBlockGroup,CensusBlock,ParcelNumberRaw,ParcelNumberFormatted,ParcelNumberYearAdded,ParcelNumberAlternate,ParcelMapBook,ParcelMapPage,ParcelNumberYearChange,ParcelNumberPrevious,ParcelAccountNumber,PropertyAddressFull,PropertyAddressHouseNumber,PropertyAddressStreetDirection,PropertyAddressStreetName,PropertyAddressStreetSuffix,PropertyAddressStreetPostDirection,PropertyAddressUnitPrefix,PropertyAddressUnitValue,PropertyAddressCity,PropertyAddressState,PropertyAddressZIP,PropertyAddressZIP4,PropertyAddressCRRT,PropertyAddressInfoPrivacy,CongressionalDistrictHouse,PropertyLatitude,PropertyLongitude,GeoQuality,LegalDescription,LegalRange,LegalTownship,LegalSection,LegalQuarter,LegalQuarterQuarter,LegalSubdivision,LegalPhase,LegalTractNumber,LegalBlock1,LegalBlock2,LegalLotNumber1,LegalLotNumber2,LegalLotNumber3,LegalUnit,PartyOwner1NameFull,PartyOwner1NameFirst,PartyOwner1NameMiddle,PartyOwner1NameLast,PartyOwner1NameSuffix,TrustDescription,CompanyFlag,PartyOwner2NameFull,PartyOwner2NameFirst,PartyOwner2NameMiddle,PartyOwner2NameLast,PartyOwner2NameSuffix,OwnerTypeDescription1,OwnershipVestingRelationCode,PartyOwner3NameFull,PartyOwner3NameFirst,PartyOwner3NameMiddle,PartyOwner3NameLast,PartyOwner3NameSuffix,PartyOwner4NameFull,PartyOwner4NameFirst,PartyOwner4NameMiddle,PartyOwner4NameLast,PartyOwner4NameSuffix,OwnerTypeDescription2,ContactOwnerMailingCounty,ContactOwnerMailingFIPS,ContactOwnerMailAddressFull,ContactOwnerMailAddressHouseNumber,ContactOwnerMailAddressStreetDirection,ContactOwnerMailAddressStreetName,ContactOwnerMailAddressStreetSuffix,ContactOwnerMailAddressStreetPostDirection,ContactOwnerMailAddressUnitPrefix,ContactOwnerMailAddressUnit,ContactOwnerMailAddressCity,ContactOwnerMailAddressState,ContactOwnerMailAddressZIP,ContactOwnerMailAddressZIP4,ContactOwnerMailAddressCRRT,ContactOwnerMailAddressInfoFormat,ContactOwnerMailInfoPrivacy,StatusOwnerOccupiedFlag,DeedOwner1NameFull,DeedOwner1NameFirst,DeedOwner1NameMiddle,DeedOwner1NameLast,DeedOwner1NameSuffix,DeedOwner2NameFull,DeedOwner2NameFirst,DeedOwner2NameMiddle,DeedOwner2NameLast,DeedOwner2NameSuffix,DeedOwner3NameFull,DeedOwner3NameFirst,DeedOwner3NameMiddle,DeedOwner3NameLast,DeedOwner3NameSuffix,DeedOwner4NameFull,DeedOwner4NameFirst,DeedOwner4NameMiddle,DeedOwner4NameLast,DeedOwner4NameSuffix,TaxYearAssessed,TaxAssessedValueTotal,TaxAssessedValueImprovements,TaxAssessedValueLand,TaxAssessedImprovementsPerc,PreviousAssessedValue,TaxMarketValueYear,TaxMarketValueTotal,TaxMarketValueImprovements,TaxMarketValueLand,TaxMarketImprovementsPerc,TaxFiscalYear,TaxRateArea,TaxBilledAmount,TaxDelinquentYear,LastAssessorTaxRollUpdate,AssrLastUpdated,TaxExemptionHomeownerFlag,TaxExemptionDisabledFlag,TaxExemptionSeniorFlag,TaxExemptionVeteranFlag,TaxExemptionWidowFlag,TaxExemptionAdditional,YearBuilt,YearBuiltEffective,ZonedCodeLocal,PropertyUseMuni,PropertyUseGroup,PropertyUseStandardized,AssessorLastSaleDate,AssessorLastSaleAmount,AssessorPriorSaleDate,AssessorPriorSaleAmount,LastOwnershipTransferDate,LastOwnershipTransferDocumentNumber,LastOwnershipTransferTransactionID,DeedLastSaleDocumentBook,DeedLastSaleDocumentPage,DeedLastDocumentNumber,DeedLastSaleDate,DeedLastSalePrice,DeedLastSaleTransactionID,AreaBuilding,AreaBuildingDefinitionCode,AreaGross,Area1stFloor,Area2ndFloor,AreaUpperFloors,AreaLotAcres,AreaLotSF,AreaLotDepth,AreaLotWidth,RoomsAtticArea,RoomsAtticFlag,RoomsBasementArea,RoomsBasementAreaFinished,RoomsBasementAreaUnfinished,ParkingGarage,ParkingGarageArea,ParkingCarport,ParkingCarportArea,HVACCoolingDetail,HVACHeatingDetail,HVACHeatingFuel,UtilitiesSewageUsage,UtilitiesWaterSource,UtilitiesMobileHomeHookupFlag,Foundation,Construction,InteriorStructure,PlumbingFixturesCount,ConstructionFireResistanceClass,SafetyFireSprinklersFlag,FlooringMaterialPrimary,BathCount,BathPartialCount,BedroomsCount,RoomsCount,StoriesCount,UnitsCount,RoomsBonusRoomFlag,RoomsBreakfastNookFlag,RoomsCellarFlag,RoomsCellarWineFlag,RoomsExerciseFlag,RoomsFamilyCode,RoomsGameFlag,RoomsGreatFlag,RoomsHobbyFlag,RoomsLaundryFlag,RoomsMediaFlag,RoomsMudFlag,RoomsOfficeArea,RoomsOfficeFlag,RoomsSafeRoomFlag,RoomsSittingFlag,RoomsStormShelter,RoomsStudyFlag,RoomsSunroomFlag,RoomsUtilityArea,RoomsUtilityCode,Fireplace,FireplaceCount,AccessabilityElevatorFlag,AccessabilityHandicapFlag,EscalatorFlag,CentralVacuumFlag,ContentIntercomFlag,ContentSoundSystemFlag,WetBarFlag,SecurityAlarmFlag,StructureStyle,Exterior1Code,RoofMaterial,RoofConstruction,ContentStormShutterFlag,ContentOverheadDoorFlag,ViewDescription,PorchCode,PorchArea,PatioArea,DeckFlag,DeckArea,FeatureBalconyFlag,BalconyArea,BreezewayFlag,ParkingRVParkingFlag,ParkingSpaceCount,DrivewayArea,DrivewayMaterial,Pool,PoolArea,ContentSaunaFlag,TopographyCode,FenceCode,FenceArea,CourtyardFlag,CourtyardArea,ArborPergolaFlag,SprinklersFlag,GolfCourseGreenFlag,TennisCourtFlag,SportsCourtFlag,ArenaFlag,WaterFeatureFlag,PondFlag,BoatLiftFlag,BuildingsCount,BathHouseArea,BathHouseFlag,BoatAccessFlag,BoatHouseArea,BoatHouseFlag,CabinArea,CabinFlag,CanopyArea,CanopyFlag,GazeboArea,GazeboFlag,GraineryArea,GraineryFlag,GreenHouseArea,GreenHouseFlag,GuestHouseArea,GuestHouseFlag,KennelArea,KennelFlag,LeanToArea,LeanToFlag,LoadingPlatformArea,LoadingPlatformFlag,MilkHouseArea,MilkHouseFlag,OutdoorKitchenFireplaceFlag,PoolHouseArea,PoolHouseFlag,PoultryHouseArea,PoultryHouseFlag,QuonsetArea,QuonsetFlag,ShedArea,ShedCode,SiloArea,SiloFlag,StableArea,StableFlag,StorageBuildingArea,StorageBuildingFlag,UtilityBuildingArea,UtilityBuildingFlag,PoleStructureArea,PoleStructureFlag,
 CommunityRecRoomFlag,PublicationDate,ParcelShellRecord,Sourcefilename) 
 VALUES (source.ATTOMID,source.SitusStateCode,source.SitusCounty,source.PropertyJurisdictionName,source.SitusStateCountyFIPS,source.CombinedStatisticalArea,source.CBSAName,source.CBSACode,source.MSAName,source.MSACode,source.MetropolitanDivision,source.MinorCivilDivisionName,source.MinorCivilDivisionCode,source.NeighborhoodCode,source.CensusFIPSPlaceCode,source.CensusTract,source.CensusBlockGroup,source.CensusBlock,source.ParcelNumberRaw,source.ParcelNumberFormatted,source.ParcelNumberYearAdded,source.ParcelNumberAlternate,source.ParcelMapBook,source.ParcelMapPage,source.ParcelNumberYearChange,source.ParcelNumberPrevious,source.ParcelAccountNumber,source.PropertyAddressFull,source.PropertyAddressHouseNumber,source.PropertyAddressStreetDirection,source.PropertyAddressStreetName,source.PropertyAddressStreetSuffix,source.PropertyAddressStreetPostDirection,source.PropertyAddressUnitPrefix,source.PropertyAddressUnitValue,source.PropertyAddressCity,source.PropertyAddressState,source.PropertyAddressZIP,source.PropertyAddressZIP4,source.PropertyAddressCRRT,source.PropertyAddressInfoPrivacy,source.CongressionalDistrictHouse,source.PropertyLatitude,source.PropertyLongitude,source.GeoQuality,source.LegalDescription,source.LegalRange,source.LegalTownship,source.LegalSection,source.LegalQuarter,source.LegalQuarterQuarter,source.LegalSubdivision,source.LegalPhase,source.LegalTractNumber,source.LegalBlock1,source.LegalBlock2,source.LegalLotNumber1,source.LegalLotNumber2,source.LegalLotNumber3,source.LegalUnit,source.PartyOwner1NameFull,source.PartyOwner1NameFirst,source.PartyOwner1NameMiddle,source.PartyOwner1NameLast,source.PartyOwner1NameSuffix,source.TrustDescription,source.CompanyFlag,source.PartyOwner2NameFull,source.PartyOwner2NameFirst,source.PartyOwner2NameMiddle,source.PartyOwner2NameLast,source.PartyOwner2NameSuffix,source.OwnerTypeDescription1,source.OwnershipVestingRelationCode,source.PartyOwner3NameFull,source.PartyOwner3NameFirst,source.PartyOwner3NameMiddle,source.PartyOwner3NameLast,source.PartyOwner3NameSuffix,source.PartyOwner4NameFull,source.PartyOwner4NameFirst,source.PartyOwner4NameMiddle,source.PartyOwner4NameLast,source.PartyOwner4NameSuffix,source.OwnerTypeDescription2,source.ContactOwnerMailingCounty,source.ContactOwnerMailingFIPS,source.ContactOwnerMailAddressFull,source.ContactOwnerMailAddressHouseNumber,source.ContactOwnerMailAddressStreetDirection,source.ContactOwnerMailAddressStreetName,source.ContactOwnerMailAddressStreetSuffix,source.ContactOwnerMailAddressStreetPostDirection,source.ContactOwnerMailAddressUnitPrefix,source.ContactOwnerMailAddressUnit,source.ContactOwnerMailAddressCity,source.ContactOwnerMailAddressState,source.ContactOwnerMailAddressZIP,source.ContactOwnerMailAddressZIP4,source.ContactOwnerMailAddressCRRT,source.ContactOwnerMailAddressInfoFormat,source.ContactOwnerMailInfoPrivacy,source.StatusOwnerOccupiedFlag,source.DeedOwner1NameFull,source.DeedOwner1NameFirst,source.DeedOwner1NameMiddle,source.DeedOwner1NameLast,source.DeedOwner1NameSuffix,source.DeedOwner2NameFull,source.DeedOwner2NameFirst,source.DeedOwner2NameMiddle,source.DeedOwner2NameLast,source.DeedOwner2NameSuffix,source.DeedOwner3NameFull,source.DeedOwner3NameFirst,source.DeedOwner3NameMiddle,source.DeedOwner3NameLast,source.DeedOwner3NameSuffix,source.DeedOwner4NameFull,source.DeedOwner4NameFirst,source.DeedOwner4NameMiddle,source.DeedOwner4NameLast,source.DeedOwner4NameSuffix,source.TaxYearAssessed,source.TaxAssessedValueTotal,source.TaxAssessedValueImprovements,source.TaxAssessedValueLand,source.TaxAssessedImprovementsPerc,source.PreviousAssessedValue,source.TaxMarketValueYear,source.TaxMarketValueTotal,source.TaxMarketValueImprovements,source.TaxMarketValueLand,source.TaxMarketImprovementsPerc,source.TaxFiscalYear,source.TaxRateArea,source.TaxBilledAmount,source.TaxDelinquentYear,source.LastAssessorTaxRollUpdate,source.AssrLastUpdated,source.TaxExemptionHomeownerFlag,source.TaxExemptionDisabledFlag,source.TaxExemptionSeniorFlag,source.TaxExemptionVeteranFlag,source.TaxExemptionWidowFlag,source.TaxExemptionAdditional,source.YearBuilt,source.YearBuiltEffective,source.ZonedCodeLocal,source.PropertyUseMuni,source.PropertyUseGroup,source.PropertyUseStandardized,source.AssessorLastSaleDate,source.AssessorLastSaleAmount,source.AssessorPriorSaleDate,source.AssessorPriorSaleAmount,source.LastOwnershipTransferDate,source.LastOwnershipTransferDocumentNumber,source.LastOwnershipTransferTransactionID,source.DeedLastSaleDocumentBook,source.DeedLastSaleDocumentPage,source.DeedLastDocumentNumber,source.DeedLastSaleDate,source.DeedLastSalePrice,source.DeedLastSaleTransactionID,source.AreaBuilding,source.AreaBuildingDefinitionCode,source.AreaGross,source.Area1stFloor,source.Area2ndFloor,source.AreaUpperFloors,source.AreaLotAcres,source.AreaLotSF,source.AreaLotDepth,source.AreaLotWidth,source.RoomsAtticArea,source.RoomsAtticFlag,source.RoomsBasementArea,source.RoomsBasementAreaFinished,source.RoomsBasementAreaUnfinished,source.ParkingGarage,source.ParkingGarageArea,source.ParkingCarport,source.ParkingCarportArea,source.HVACCoolingDetail,source.HVACHeatingDetail,source.HVACHeatingFuel,source.UtilitiesSewageUsage,source.UtilitiesWaterSource,source.UtilitiesMobileHomeHookupFlag,source.Foundation,source.Construction,source.InteriorStructure,source.PlumbingFixturesCount,source.ConstructionFireResistanceClass,source.SafetyFireSprinklersFlag,source.FlooringMaterialPrimary,source.BathCount,source.BathPartialCount,source.BedroomsCount,source.RoomsCount,source.StoriesCount,source.UnitsCount,source.RoomsBonusRoomFlag,source.RoomsBreakfastNookFlag,source.RoomsCellarFlag,source.RoomsCellarWineFlag,source.RoomsExerciseFlag,source.RoomsFamilyCode,source.RoomsGameFlag,source.RoomsGreatFlag,source.RoomsHobbyFlag,source.RoomsLaundryFlag,source.RoomsMediaFlag,source.RoomsMudFlag,source.RoomsOfficeArea,source.RoomsOfficeFlag,source.RoomsSafeRoomFlag,source.RoomsSittingFlag,source.RoomsStormShelter,source.RoomsStudyFlag,source.RoomsSunroomFlag,source.RoomsUtilityArea,source.RoomsUtilityCode,source.Fireplace,source.FireplaceCount,source.AccessabilityElevatorFlag,source.AccessabilityHandicapFlag,source.EscalatorFlag,source.CentralVacuumFlag,source.ContentIntercomFlag,source.ContentSoundSystemFlag,source.WetBarFlag,source.SecurityAlarmFlag,source.StructureStyle,source.Exterior1Code,source.RoofMaterial,source.RoofConstruction,source.ContentStormShutterFlag,source.ContentOverheadDoorFlag,source.ViewDescription,source.PorchCode,source.PorchArea,source.PatioArea,source.DeckFlag,source.DeckArea,source.FeatureBalconyFlag,source.BalconyArea,source.BreezewayFlag,source.ParkingRVParkingFlag,source.ParkingSpaceCount,source.DrivewayArea,source.DrivewayMaterial,source.Pool,source.PoolArea,source.ContentSaunaFlag,source.TopographyCode,source.FenceCode,source.FenceArea,source.CourtyardFlag,source.CourtyardArea,source.ArborPergolaFlag,source.SprinklersFlag,source.GolfCourseGreenFlag,source.TennisCourtFlag,source.SportsCourtFlag,source.ArenaFlag,source.WaterFeatureFlag,source.PondFlag,source.BoatLiftFlag,source.BuildingsCount,source.BathHouseArea,source.BathHouseFlag,source.BoatAccessFlag,source.BoatHouseArea,source.BoatHouseFlag,source.CabinArea,source.CabinFlag,source.CanopyArea,source.CanopyFlag,source.GazeboArea,source.GazeboFlag,source.GraineryArea,source.GraineryFlag,source.GreenHouseArea,source.GreenHouseFlag,source.GuestHouseArea,source.GuestHouseFlag,source.KennelArea,source.KennelFlag,source.LeanToArea,source.LeanToFlag,source.LoadingPlatformArea,source.LoadingPlatformFlag,source.MilkHouseArea,source.MilkHouseFlag,source.OutdoorKitchenFireplaceFlag,source.PoolHouseArea,source.PoolHouseFlag,source.PoultryHouseArea,source.PoultryHouseFlag,source.QuonsetArea,source.QuonsetFlag,source.ShedArea,source.ShedCode,source.SiloArea,source.SiloFlag,source.StableArea,source.StableFlag,source.StorageBuildingArea,source.StorageBuildingFlag,source.UtilityBuildingArea,source.UtilityBuildingFlag,source.PoleStructureArea,source.PoleStructureFlag,source.CommunityRecRoomFlag,source.PublicationDate,
source.ParcelShellRecord, ''' + QUOTENAME(@tableName) + '''); ';

-- Execute the dynamic SQL
EXEC sp_executesql @sql;

-- Fetch the next table name
FETCH NEXT FROM table_cursor INTO @tableName;
END

-- Step 7: Close and deallocate the cursor
CLOSE table_cursor;
DEALLOCATE table_cursor;



 

 select  ATTOMID ,TaxYearAssessed,SitusCounty,SitusStateCountyFIPS, ParcelNumberRaw,ParcelNumberFormatted,ParcelNumberAlternate,
ParcelNumberPrevious,ParcelAccountNumber,PropertyAddressFull,
PropertyAddressHouseNumber,
PropertyAddressStreetDirection,
PropertyAddressStreetName,
PropertyAddressStreetSuffix,
PropertyAddressStreetPostDirection,
PropertyAddressUnitPrefix,
PropertyAddressUnitValue,
PropertyAddressCity,
PropertyAddressState,
PropertyAddressZIP,
PropertyAddressZIP4,PropertyLatitude,PropertyLongitude
 into Geocode_suganya  from    Attom_Master_Table  where TaxYearAssessed in (2023,2024)  and  
SitusStateCountyFIPS  in  ('48029','48039','48085','48113','48121','48157','48167','48201',
'48339','48439','48453','48473','48491') 





