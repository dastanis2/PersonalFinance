#****************************************************************************************
#REFERENCES
#****************************************************************************************
import datetime
import os
import pandas as pd
import Utilities
import uuid
from datetime import datetime

#****************************************************************************************
#GLOBAL VARIABLES
#   Set these variables to either empty or hard-coded values
#   These variables can change value by any function
#****************************************************************************************
AllInboundFolders = True
ConfigurationFileID = 0
Configurations_Column_CurrentFile = pd.DataFrame()
Configurations_File_CurrentFile = pd.DataFrame()
CurrentScriptFile = os.path.realpath(__file__)
ExpectedDelimiter = ''
FullPath_Bronze_Archive_CurrentSource = ''
FullPath_Silver_Error_CurrentSource = ''
FullPath_Silver_Inbound_CurrentSource = ''
InboundFileFound = False
IsValid_LogFile = False
LogEntries = []

#****************************************************************************************
#FUNCTIONS
#****************************************************************************************
def LogStep(Begin, CallStack, ExecutionGUID, Parameters, **VariedParameters): #The explicit parameters are required; anything passed into **VariedParameters is optional; different parameters may be passed into **VariedParameters
    #Even though this local function calls another of the same name in a different script, keep this local function to be able to use "**VariedParameters"
    #Variable(s) defined outside of this function, but set within this function
    global LogEntries
    #Add the step to the current set
    LogEntries = Utilities.LogStep(Begin, CurrentScriptFile, CallStack, ExecutionGUID, LogEntries, Parameters, **VariedParameters)

def Main(InboundSourceFolder = '', ParentExecutionGUID = ''):
    #Variable(s) defined outside of this function, but set within this function
    global LogEntries

    #Local variables
    Begin = datetime.now()
    CurrentFunction = r'Main'
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'InboundSourceFolder': InboundSourceFolder
        , 'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Utilities.Result_Success

    #Perform the actions that could cause the entire script to fail; if this block fails, report the failure, don't log the failure, and don't continue
    try:
        #Set global variables to be used downstream
        LogEntries, Result = Utilities.SetGlobalVariables(CurrentScriptFile, CurrentFunction, LogEntries, ExecutionGUID)
        if(Result != Utilities.Result_Success): raise Exception('Error in SetGlobalVariables') #Log the error and don't continue

        #Set script-wide calculated variables - included in the "try/except" block in case an expression fails
        if(InboundSourceFolder == ''): AllInboundFolders = True #If no source folder was specified
        elif(Utilities.FullPath_Root not in InboundSourceFolder): AllInboundFolders = False #If source folder was specified, but not the full path (as expected)

        #Validate the log file so that all subsequent steps & errors can be properly logged
        LogEntries, Result = Utilities.ValidateLogFile(CurrentFunction, LogEntries, ExecutionGUID)
        if(Result != Utilities.Result_Success): raise Exception(Result) #Report the error and don't continue - can't log the error because log file is invalid

        #Get all file level configurations; do this only once per process execution
        LogEntries, Result = Utilities.RetrieveConfigurations_File(CurrentFunction, LogEntries, ExecutionGUID)
        if(Result != Utilities.Result_Success): raise Exception('Error in RetrieveConfigurations_File') #Log the error and don't continue

        #Get all column level configurations for the current ConfigurationFileID; do this only once per process execution
        LogEntries, Result = Utilities.RetrieveConfigurations_Column(CurrentFunction, LogEntries, ExecutionGUID)
        if(Result != Utilities.Result_Success): raise Exception('Error in RetrieveConfigurations_Column') #Log the error and don't continue

        #Validate root level parameters
        Result = ValidateRootParameters(CurrentFunction, InboundSourceFolder, ExecutionGUID)
        if(Result != Utilities.Result_Success): raise Exception('Error in ValidateRootParameters') #Log the error and don't continue

        #Validate Silver Dimensions
#        Result, LogEntries, FullPath_Silver_Dimension_Account, SilverDimension_Account = Utilities.ValidateSilverDimension(CurrentFunction, 'Account', LogEntries, ParentExecutionGUID)
#        Result, LogEntries, FullPath_Silver_Dimension_AccountType, SilverDimension_AccountType = Utilities.ValidateSilverDimension(CurrentFunction, 'AccountType', LogEntries, ParentExecutionGUID)
#        Result, LogEntries, FullPath_Silver_Dimension_Brand, SilverDimension_Brand = Utilities.ValidateSilverDimension(CurrentFunction, 'Brand', LogEntries, ParentExecutionGUID)
#        Result, LogEntries, FullPath_Silver_Dimension_Category, SilverDimension_Category = Utilities.ValidateSilverDimension(CurrentFunction, 'Category', LogEntries, ParentExecutionGUID)
#        Result, LogEntries, FullPath_Silver_Dimension_PriceType, SilverDimension_PriceType = Utilities.ValidateSilverDimension(CurrentFunction, 'PriceType', LogEntries, ParentExecutionGUID)
#        Result, LogEntries, FullPath_Silver_Dimension_ProductService, SilverDimension_ProductService = Utilities.ValidateSilverDimension(CurrentFunction, 'ProductService', LogEntries, ParentExecutionGUID)
#        Result, LogEntries, FullPath_Silver_Dimension_Seller, SilverDimension_Seller = Utilities.ValidateSilverDimension(CurrentFunction, 'Seller', LogEntries, ParentExecutionGUID)
#        Result, LogEntries, FullPath_Silver_Dimension_UnitOfMeasurement, SilverDimension_UnitOfMeasurement = Utilities.ValidateSilverDimension(CurrentFunction, 'UnitOfMeasurement', LogEntries, ParentExecutionGUID)

    except Exception as e:
        print(Utilities.BuildErrorMessage(CurrentFunction, CurrentScriptFile, e, False, Parameters)) #Report the error

    if(Utilities.IsValid_LogFile):
        #Now process all Inbound folders, along with all necessary validations
        try:
            if AllInboundFolders: #Process all Inbound sub-folders
#                for FolderName in os.listdir(Utilities.FullPath_Silver_Inbound):
                for FolderName in Utilities.Configurations_File_All['Source'].unique():
                    Result = ProcessInboundFolder(CurrentFunction, ExecutionGUID, FolderName)
            else: #Process just the specified Inbound sub-folder
                Result = ProcessInboundFolder(CurrentFunction, ExecutionGUID, InboundSourceFolder)

            if not InboundFileFound: Result = Utilities.Result_Success #If no Inbound files were found, set the result to success so that the script doesn't fail; this is not an error, just a condition

            #Log the step
            LogStep(Begin, CurrentFunction, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Utilities.Severity_Info)

        except Exception as e:
            Result = Utilities.BuildErrorMessage(CurrentFunction, CurrentScriptFile, e, True) #Build the error message
            LogStep(Begin, CurrentFunction, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Utilities.Severity_Error) #Log the error
            print(Result) #Report the error

        finally:
            Utilities.WriteToLogFile(CurrentFunction, LogEntries, ExecutionGUID)

def ProcessInboundFile(BronzeData, CallStack, InboundFile, ParentExecutionGUID, Source):
    #Variable(s) defined outside of this function, but set within this function
    global LogEntries

    #Local variables
    Begin = datetime.now()
    InboundFile = ''
    CurrentFunction = 'ProcessInboundFile'
    CallStack = f'{CallStack} > {CurrentFunction}' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'InboundFile': InboundFile
        , 'ParentExecutionGUID': ParentExecutionGUID
        , 'Source': Source
    }
    Result = Utilities.Result_Success
    try:
        FileName = os.path.basename(InboundFile) #Get the file name from the full path
        FileExtension = os.path.splitext(FileName)[1] #Get the file extension of the current file

        #Process only .csv or .txt files
        if FileName.lower().endswith(('.csv', '.txt')):
            #Read the current file into a dataframe
            CurrentFile = pd.read_csv(InboundFile, delimiter = ExpectedDelimiter)

            #Remove duplicate rows from the file
            CurrentFile.drop_duplicates()

            if not CurrentFile.empty:
                #Add metadata columns to the Bronze data
                CurrentFile['DateTimeInserted'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') #The current date and time
                CurrentFile['ExecutionGUID'] = ExecutionGUID
                CurrentFile['SourceFile'] = FileName

                #Transform the Bronze files into Silver data entities
                Result = TransformBronzeToSilver(CurrentFile, CallStack, ParentExecutionGUID, CurrentFile)
                if(Result != Utilities.Result_Success): raise Exception('Error in TransformBronzeToSilver') #Log the error and don't continue

        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, File = InboundFile, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Utilities.Severity_Info)

    except Exception as e:
        Result = Utilities.BuildErrorMessage(CurrentFunction, CurrentScriptFile, e, True) #Build the error message
        LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Utilities.Severity_Error) #Log the error
        print(Result) #Report the error

    finally:
        #Return the result
        return Result

def ProcessInboundFolder(CallStack, ParentExecutionGUID, Source):
    #Variable(s) defined outside of this function, but set within this function
    global ConfigurationFileID
    global Configurations_Column_CurrentFile
    global Configurations_File_CurrentFile
    global ExpectedDelimiter
    global FullPath_Bronze_Archive_CurrentSource
    global FullPath_Silver_Error_CurrentSource
    global FullPath_Silver_Inbound_CurrentSource
    global InboundFileFound
    global LogEntries

    #Local variables
    Begin = datetime.now()
    BronzeData = pd.DataFrame() #Dataframe to hold all Bronze files for the current source
    CurrentFunction = 'ProcessInboundFolder'
    CallStack = f'{CallStack} > {CurrentFunction}' #Add the current function to the call stack
    EmptyFolder = ''
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    InboundFileFound = False
    Parameters = {
        'ParentExecutionGUID': ParentExecutionGUID
        , 'Source': Source
    }
    Result = Utilities.Result_Success
    try:
#        InboundFolder = os.path.join(Utilities.FullPath_Silver_Inbound, Source)
        LogEntries, Result, InboundFolder = Utilities.BuildFolderPath(CallStack, Utilities.FullPath_Bronze_Inbound, Source, LogEntries, ParentExecutionGUID)

        #Loop through all files in the current Inbound sub-folder
        if not os.listdir(InboundFolder):
            #There are no files in the folder
            InboundFileFound = False
            if(EmptyFolder != ''): EmptyFolder = EmptyFolder + ', '
            EmptyFolder = EmptyFolder + InboundFolder
            Result = 'No files were found for processing in ' + EmptyFolder
        else:
            #There are files in the folder
            InboundFileFound = True

            #Set & validate the full paths of subfolders for the current source
            LogEntries, Result, FullPath_Bronze_Archive_CurrentSource = Utilities.BuildFolderPath(CallStack, Utilities.FullPath_Bronze_Archive, Source, LogEntries, ParentExecutionGUID)
            LogEntries, Result, FullPath_Silver_Error_CurrentSource = Utilities.BuildFolderPath(CallStack, Utilities.FullPath_Bronze_Error, Source, LogEntries, ParentExecutionGUID)
            LogEntries, Result, FullPath_Silver_Inbound_CurrentSource = Utilities.BuildFolderPath(CallStack, Utilities.FullPath_Silver_Inbound, Source, LogEntries, ParentExecutionGUID)

            #Filter file level configurations by Source
            Configurations_File_CurrentFile = Utilities.Configurations_File_All[Utilities.Configurations_File_All['Source'] == Source]
            if(len(Configurations_File_CurrentFile) == 0):
                #No file level configurations were found for the current source
                Result = f'No configuration records were found for source "{Source}" in file {Utilities.FullPath_Configurations_File}'
                LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Utilities.Severity_Error)
            else:
                #Get ConfigurationID from the file level configurations for the current source
                ConfigurationFileID = Configurations_File_CurrentFile[['ConfigurationFileID']].iloc[0,0] #Use .iloc[0,0] to pinpoint the exact cell and exclude the column header in the return value

                #Get Delimiter of current file from file level configurations
                ExpectedDelimiter = Configurations_File_CurrentFile[['Delimiter']].iloc[0,0]

                #Filter column level configurations by ConfigurationFileID
                Configurations_Column_CurrentFile = Utilities.Configurations_Column_All[Utilities.Configurations_Column_All['ConfigurationFileID'] == int(ConfigurationFileID)]

                if(len(Configurations_Column_CurrentFile) == 0):
                    #No column level configurations were found for the current source
                    Result = f'No configuration records were found for ConfigurationFileID {ConfigurationFileID} in file {Utilities.FullPath_Configurations_Column}'
                    LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Utilities.Severity_Error)
                else:
                    #Append each file in the current Inbound folder to the BronzeData dataframe
                    for FileName in os.listdir(InboundFolder):
                        Result = ProcessInboundFile(BronzeData, CallStack, FileName, ExecutionGUID, Source)
                        if(Result != Utilities.Result_Success): raise Exception('Error in ProcessInboundFile') #Log the error and don't continue

        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Utilities.Severity_Info)

    except Exception as e:
        Result = Utilities.BuildErrorMessage(CurrentFunction, CurrentScriptFile, e, True) #Build the error message
        LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Utilities.Severity_Error) #Log the error
        print(Result) #Report the error

    finally:
        #Return the result
        return Result

def TransformBronzeToSilver(BronzeData, CallStack, ParentExecutionGUID, SourceFile):
    #Local variables
    Begin = datetime.now()
    CurrentFunction = 'TransformBronzeToSilver'
    CallStack = f'{CallStack} > {CurrentFunction}' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Utilities.Result_Success
    try:
        def ProcessDimension(BronzeDataTransformed, Dimension, Lookup, SourceFile):
            SilverDimension = pd.DataFrame() #Initialize the Silver Dimension dataframe
            FullPath_Silver_Dimension = '' #Initialize the full path to the Silver Dimension file
            #Validate the Silver Dimension
            LogEntries, Result, FullPath_Silver_Dimension, SilverDimension = Utilities.ValidateSilverDimension(CallStack, Dimension, LogEntries, ParentExecutionGUID)
            if(Result != Utilities.Result_Success): raise Exception('Error in ValidateSilverDimension') #Log the error and don't continue

            #Process direct copies, no transformations
#            if(TransformationConfigurations[TransformationConfigurations[['Transformation_BronzeToSilver_Type']] == 'Direct']):

            #Process transformations built with expressions
#            if(TransformationConfigurations[TransformationConfigurations[['Transformation_BronzeToSilver_Type']] == 'Expression']):

            #Process lookups to Silver dimensions
            if(TransformationConfigurations[TransformationConfigurations[['Transformation_BronzeToSilver_Type']] == 'Lookup']):
                TransformationConfigurations_Dimension = TransformationConfigurations[TransformationConfigurations[['SilverEntity']] == f'{Dimension}GUID']
                if(not Lookup.empty) and (TransformationConfigurations_Dimension[['Transformation_BronzeToSilver_Type']].iloc[0, 0] == 'Lookup'): 
                    BronzeDataTransformed[f'{Dimension}GUID'] = pd.merge(BronzeDataTransformed, Lookup, left_on = TransformationConfigurations_Dimension[['ColumnName_Bronze']].iloc[0,0], right_on = 'Name', how = 'left')[f'{Dimension}GUID']
            
            if(not SilverDimension.empty):
                DimensionNaturalKey = TransformationConfigurations[TransformationConfigurations[['ColumnIsInNaturalKey']] == 'Yes']
                DimensionNaturalKeyList = DimensionNaturalKey['SilverColumn'].values.tolist() #Get the list of columns that are in the natural key
                SourceFileNaturalKeyList = DimensionNaturalKey['ColumnName_Bronze'].values.tolist() #Get the list of columns that are in the natural key from the source file

                #Remove from the Bronze data that already exists in the Silver Dimension
                ToAppend = BronzeDataTransformed[~BronzeDataTransformed[[f'{SourceFileNaturalKeyList}']].apply(tuple, 1).isnotin(SilverDimension[[f'{DimensionNaturalKeyList}']].apply(tuple, 1))]

                #Append the transformed data to the Silver Dimension
                if(not ToAppend.empty): ToAppend.to_csv(FullPath_Silver_Dimension, mode = 'a', header = False, index = False, sep = Utilities.DelimiterDefault)

                #Log the step
                LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, RowCount = len(ToAppend), Severity = Utilities.Severity_Info), Source = SourceFile, Target = FullPath_Silver_Dimension

        #Load lookup dependents
        Account = pd.read_csv(Utilities.FullPath_Silver_Dimension_Account, delimiter = Utilities.DelimiterDefault)
        Account = Account[['Name'] == Configurations_File_CurrentFile[['Account']].iloc[0, 0]] #Filter the Account dataframe by the AccountName from the file level configurations
        AccountGUID = Account['AccountGUID'].iloc[0, 0] if not Account.empty else None #Get the AccountGUID from the filtered dataframe
        Brand = pd.read_csv(Utilities.FullPath_Silver_Dimension_Brand, delimiter = Utilities.DelimiterDefault)
        Category = pd.read_csv(Utilities.FullPath_Silver_Dimension_Category, delimiter = Utilities.DelimiterDefault)
#        PriceType = pd.read_csv(Utilities.FullPath_Silver_Dimension_PriceType, delimiter = Utilities.DelimiterDefault)
        ProductService = pd.read_csv(Utilities.FullPath_Silver_Dimension_ProductService, delimiter = Utilities.DelimiterDefault)
        Seller = pd.read_csv(Utilities.FullPath_Silver_Dimension_Seller, delimiter = Utilities.DelimiterDefault)
#        UnitOfMeasurement = pd.read_csv(Utilities.FullPath_Silver_Dimension_UnitOfMeasurement, delimiter = Utilities.DelimiterDefault)

        #Filter for only columns that are mapped to Silver columns
        TransformationConfigurations = Configurations_Column_CurrentFile[Configurations_Column_CurrentFile[['SilverEntity']] != '']

        #Process ToMap so that BrandCategoryProductServiceSeller is up to date
        ToMap = pd.read_csv(Utilities.FullPath_Configurations_ToMap, delimiter = Utilities.DelimiterDefault)
        ToMap.drop_duplicates(subset = ['AccountGUID', 'SourceValue'], inplace = True, keep = 'first') #Drop duplicates based on the foreign keys
        if(not ToMap.empty):
            #Get foreign keys
            ToMap['AccountGUID'] = AccountGUID
            ToMap['BrandGUID'] = pd.merge(ToMap, Brand, on = 'Brand', how = 'inner')[['BrandGUID']] #Join on Brand to get BrandGUID
            ToMap['CategoryGUID'] = pd.merge(ToMap, Category, on = 'Category', how = 'inner')[['CategoryGUID']] #Join on Category to get CategoryGUID
            ToMap['ProductServiceGUID'] = pd.merge(ToMap, ProductService, on = 'ProductService', how = 'inner')[['ProductServiceGUID']] #Join on ProductService to get ProductServiceGUID
            ToMap['SellerGUID'] = pd.merge(ToMap, Seller, on = 'Seller', how = 'inner')[['SellerGUID']] #Join on Seller to get SellerGUID

            #Remove already existing
            BrandCategoryProductServiceSeller = pd.read_csv(Utilities.FullPath_Configurations_BrandCategoryProductServiceSeller, delimiter = Utilities.DelimiterDefault)
            ToMap = ToMap[~ToMap[['AccountGUID', 'BrandGUID', 'CategoryGUID', 'ProductServiceGUID', 'SellerGUID']].apply(tuple, 1).isnotin(BrandCategoryProductServiceSeller[['AccountGUID', 'BrandGUID', 'CategoryGUID', 'ProductServiceGUID', 'SellerGUID']].apply(tuple, 1))]

            #Append the new mappings to the BrandCategoryProductServiceSeller file
            if(not ToMap.empty): ToMap.to_csv(Utilities.FullPath_Configurations_BrandCategoryProductServiceSeller, mode = 'a', header = False, index = False, sep = Utilities.DelimiterDefault)

            #Remove just appended
            BrandCategoryProductServiceSeller = pd.read_csv(Utilities.FullPath_Configurations_BrandCategoryProductServiceSeller, delimiter = Utilities.DelimiterDefault)
            ToMap = ToMap[~ToMap[['AccountGUID', 'BrandGUID', 'CategoryGUID', 'ProductServiceGUID', 'SellerGUID']].apply(tuple, 1).isnotin(BrandCategoryProductServiceSeller[['AccountGUID', 'BrandGUID', 'CategoryGUID', 'ProductServiceGUID', 'SellerGUID']].apply(tuple, 1))]

            BronzeData_CannotProcess = BronzeData.copy() #Create a copy of the Bronze data that cannot be processed
            BronzeData_CannotProcess = BronzeData_CannotProcess[~BronzeData_CannotProcess[['SourceFile']].apply(tuple, 1).isnotin(ToMap[['SourceFile']].apply(tuple, 1))]

            #Write the Bronze data that cannot be processed to the ToMap file
            if(not BronzeData_CannotProcess.empty): BronzeData_CannotProcess.to_csv(Utilities.FullPath_Configurations_ToMap, mode = 'a', header = False, index = False, sep = Utilities.DelimiterDefault)

            #Remove the Bronze data that cannot be processed from the original Bronze data
            BronzeData = BronzeData[~BronzeData[['SourceFile']].apply(tuple, 1).isnotin(BronzeData_CannotProcess[['SourceFile']].apply(tuple, 1))]

            #Create a copy of the Bronze data that will be transformed into Silver data
            BronzeDataTransformed = BronzeData.copy()

            BronzeDataTransformed = ProcessDimension(BronzeDataTransformed, 'Brand', Brand, SourceFile)
            BronzeDataTransformed = ProcessDimension(BronzeDataTransformed, 'Category', Category, SourceFile)
            BronzeDataTransformed = ProcessDimension(BronzeDataTransformed, 'ProductService', ProductService, SourceFile)
            BronzeDataTransformed = ProcessDimension(BronzeDataTransformed, 'Seller', Seller, SourceFile)

            #

#        for Dimension in Utilities.Silver_Dimensions:
#            SilverDimension = pd.DataFrame() #Initialize the Silver Dimension dataframe
#            FullPath_Silver_Dimension = '' #Initialize the full path to the Silver Dimension file
#            #Validate the Silver Dimension
#            LogEntries, Result, FullPath_Silver_Dimension, SilverDimension = Utilities.ValidateSilverDimension(CallStack, Dimension, LogEntries, ParentExecutionGUID)
#            if(Result != Utilities.Result_Success): raise Exception('Error in ValidateSilverDimension') #Log the error and don't continue

#            #Create a copy of the Bronze data that will be transformed into Silver data
#            BronzeDataTransformed = BronzeData.copy()

            #Process direct copies, no transformations
#            if(TransformationConfigurations[TransformationConfigurations[['Transformation_BronzeToSilver_Type']] == 'Direct']):

            #Process transformations built with expressions
#            if(TransformationConfigurations[TransformationConfigurations[['Transformation_BronzeToSilver_Type']] == 'Expression']):

#            #Process lookups to Silver dimensions
#            if(TransformationConfigurations[TransformationConfigurations[['Transformation_BronzeToSilver_Type']] == 'Lookup']):
#                TransformationConfigurations_Dimension = TransformationConfigurations[TransformationConfigurations[['SilverEntity']] == f'{Dimension}GUID']
#                Lookup = pd.DataFrame()
#                if(Dimension == 'Brand'): Lookup = Brand
#                elif(Dimension == 'Category'): Lookup = Category
#                elif(Dimension == 'ProductService'): Lookup = ProductService
#                elif(Dimension == 'Seller'): Lookup = Seller
#                if(not Lookup.empty) and (TransformationConfigurations_Dimension[['Transformation_BronzeToSilver_Type']].iloc[0,0] == 'Lookup'): 
#                    BronzeDataTransformed[f'{Dimension}GUID'] = pd.merge(BronzeDataTransformed, Lookup, left_on = TransformationConfigurations_Dimension[['ColumnName_Bronze']].iloc[0,0], right_on = 'Name', how = 'left')[f'{Dimension}GUID']
                

#            if Dimension == 'Brand':
##                ToProcess = BronzeData.copy()[Utilities.Silver_Dimension_Definition_Brand]
#                TransformationConfigurations_Brand = TransformationConfigurations[TransformationConfigurations[['SilverColumn']] == 'BrandGUID']
#                #Transform received value into Silver value; lookup the GUID from the Silver dimension
#                if(TransformationConfigurations_Brand[['Transformation_BronzeToSilver_Type']].iloc[0,0] == 'Lookup'): 
#                    BronzeDataTransformed['BrandGUID'] = pd.merge(BronzeDataTransformed, Brand, left_on = TransformationConfigurations_Brand[['ColumnName_Bronze']].iloc[0,0], right_on = 'Name', how = 'left')['BrandGUID']
            

            #Apply any formulas to the columns in BronzeData (such as data type conversions, column replacments, etc.)
#            for _, config_row in TransformationConfiguration.drop_duplicates(subset = ['ColumnName_Bronze']).iterrows():
#                ColumnName_Bronze = config_row['ColumnName_Bronze']
#                Formula = config_row.get('Transformation_BronzeToSilver', None)
#                if ColumnName_Bronze and isinstance(Formula, str) and Formula.strip(): #Only create and populate if there is a formula
#                    BronzeDataTransformed[ColumnName_Bronze] = BronzeDataTransformed.apply(
#                        lambda row: eval(Formula, {'row': row, 'pd': pd, 'IngestDatetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}) #Put the formula for IngestDatetime here so that it is written only once (instead of multiple times in the column level configuration file) but applied to every row, regardless of file or source
#                        , axis = 1
#                    )

#            #Map columns from File to Bronze without creating duplicates
#            Seen = set()
#            UniqueMapping = {}
#            for ColumnName_Source, ColumnName_Target in zip(TransformationConfiguration['ColumnName_File'], TransformationConfiguration['ColumnName_Silver']):
#                if pd.notnull(ColumnName_Target) and pd.notnull(ColumnName_Target) and ColumnName_Target not in Seen:
#                    UniqueMapping[ColumnName_Source] = ColumnName_Target
#                    Seen.add(ColumnName_Target)
#            ColumnMapping = UniqueMapping
#            BronzeDataTransformed = BronzeDataTransformed.rename(columns = ColumnMapping)

#            #Align RecordsToCopy to ExistingBronze columns by dropping any columns not in ExistingBronze
##            Bronze_Transaction = pd.read_csv(Utilities.FullPath_Bronze_Transaction, delimiter = Utilities.DelimiterDefault)
#            BronzeDataTransformed = BronzeDataTransformed[[col for col in SilverDimension.columns if col in BronzeDataTransformed.columns]]


    except Exception as e:
        Result = Utilities.BuildErrorMessage(CurrentFunction, CurrentScriptFile, e, True) #Build the error message
        LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Utilities.Severity_Error) #Log the error
        print(Result) #Report the error

    finally:
        #Return the result
        return Result

def ValidateRootParameters(CallStack, InboundSourceFolder, ParentExecutionGUID):
    #Local variables
    Begin = datetime.now()
    CurrentFunction = 'ValidateRootParameters'
    CallStack = f'{CallStack} > {CurrentFunction}' #Add the current function to the call stack
    Continue = True
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'InboundSourceFolder': InboundSourceFolder
        , 'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Utilities.Result_Success
    try:
        #Validate that each required parameter has a value; don't explicitly raise any excpetion if an issue is found, just log it and move on to the next parameter
        if Continue:
            #Validate folders
            if(InboundSourceFolder != '') and not os.path.exists(InboundSourceFolder):
                os.mkdir(InboundSourceFolder) #If a value was provided but doesn't exist, create it
                Result = 'Folder path specified was not found and therefore created: ' + InboundSourceFolder #Return the warning but continue and don't fail
       
            #Log the step
            LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Utilities.Severity_Info)

    except Exception as e:
        Result = Utilities.BuildErrorMessage(CurrentFunction, CurrentScriptFile, e, True) #Build the error message
        LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Utilities.Severity_Error) #Log the error
        print(Result) #Report the error

    finally:
        #Return the result
        return Result

#****************************************************************************************
#ENTRY
#****************************************************************************************
if __name__ == '__main__':
    Main()

