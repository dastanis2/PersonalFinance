#****************************************************************************************
""""
DESCRIPTION
    Load all valid files into the Bronze layer 

STEPS
	1. Set script-wide calculated variables
	2. Validate the log file
	3. Validate root parameters
	4. For each sub-folder in the Inbound folder
		4.1. For each file in the current Inbound sub-folder
			4.1.1. Retrieve file level configuration values for the current file
			4.1.2. Retrieve column level configuration values for the current file
			4.1.3. Validate the column header of the current file
			4.1.4. Copy the current file to the Bronze layer
			4.1.5. Archive the current file
	5. Write all steps and errors to the log file

PARAMETERS
    ColumnConfigurationFilename
        Datatype: string
        Expected: the file name of the file containing column level configurations, excluding path (i.e. 'ConfigurationColumn.txt')

    FileConfigurationFilename
        Datatype: string
        Expected: the file name of the file containing file level configurations, excluding path (i.e. 'ConfigurationFile.txt')

    FullPath_Root
        Datatype: string
        Expected: the full path of the root folder for all files & code (i.e. 'C:\FolderA\FolderB')

    InboundSourceFolder
        Datatype: string
        Expected: either empty string, nothing, or a valid folder name in the root bronze folder (i.e. 'BankABC')
"""
#****************************************************************************************

#****************************************************************************************
#REFERENCES
#****************************************************************************************
import csv
import inspect
import logging
import os
import pandas as pd
import uuid
from datetime import datetime
from pathlib import Path

#****************************************************************************************
#PARAMETERS
#****************************************************************************************
ColumnConfigurationFilename = r''
FileConfigurationFilename = r''
FullPath_Root = r''
InboundSourceFolder = r''

#****************************************************************************************
#GLOBAL VARIABLES
#   Set these variables to either empty or hard-coded values
#   These variables can change value by any function
#****************************************************************************************
AllInboundFolders = True
Caller = os.path.realpath(__file__)
Configurations_Column_CurrentFile = pd.DataFrame()
Configurations_File_CurrentFile = pd.DataFrame()
DelimiterDefault = r'|'
FullPath_Archive = ''
FullPath_Bronze = ''
FullPath_Configurations = ''
FullPath_Configurations_File = ''
FullPath_Configurations_Column = ''
FullPath_Inbound = ''
FullPath_LogFile = ''
InboundFileFound = False
LogEntries = []
LogFileDefinition = [
    'ExecutionGUID'
    , 'ParentExecutionGUID'
    , 'Begin'
    , 'End'
    , 'Severity'
    , 'Caller'
    , 'CallStack'
    , 'Action'
    , 'RowCount'
    , 'Source'
    , 'Target'
    , 'Result'
    , 'File'
    , 'Parameters'
]
Result_Success = r'Success'
Severity_Error = r'Error'
Severity_Info = r'Info'

#****************************************************************************************
#CLASSES
#****************************************************************************************
class FailWithoutLogging(Exception):
    """Exception raised for when an error can/should not be logged."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

#****************************************************************************************
#FUNCTIONS
#****************************************************************************************
def CopyToBronze(CallStack, Configurations_Column_CurrentFile, ParentExecutionGUID, Source, ValidRecords):
    Begin = datetime.now()
    BronzeFile = os.path.join(FullPath_Bronze, f'Bronze.{Source}.txt')
    CallStack = CallStack + ' > CopyToBronze' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'ParentExecutionGUID': ParentExecutionGUID
        , 'Source': Source
    }
    Result = Result_Success
    try:
        #Get all existing Bronze records for comparison
        ExistingBronze = pd.read_csv(BronzeFile, delimiter = DelimiterDefault)
        RecordsToCopy = ValidRecords

        #Rename the columns to match ColumnName_Bronze from the column level configurations
        ColumnMapping = dict_df = dict(zip(Configurations_Column_CurrentFile['ColumnName_File'], Configurations_Column_CurrentFile['ColumnName_Bronze']))
        RecordsToCopy.rename(columns = ColumnMapping, inplace = True)

        #Remove duplicates
        RecordsToCopy.drop_duplicates(keep = 'last', inplace = True)

        #Remove records already in Bronze
        if len(ExistingBronze) > 0: RecordsToCopy = RecordsToCopy.merge(ExistingBronze, indicator = True, how = 'outer').query('_merge == "left_only"').drop('_merge', axis = 1)

        #Copy the records to the Bronze file
        RecordsToCopy.to_csv(BronzeFile, sep = DelimiterDefault, header = False, index = False, mode = 'a')

        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)

    except Exception as e:
        #Return the error
        Result = str(e)

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

        #Report the error
        print('Error in CopyToBronze on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result

def LogStep(Begin, CallStack, ExecutionGUID, Parameters, **VariedParameters): #The explicit parameters are required; anything passed into **VariedParameters is optional; different parameters may be passed into **VariedParameters
    #Don't log this function
    try:
        Begin = Begin.strftime('%Y-%m-%d %H:%M:%S.%f') #Format the value as YYYY-MM-DD HH:MM:SS.ms
        End = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') #Format the value as YYYY-MM-DD HH:MM:SS.ms
        if(ExecutionGUID == ''): ExecutionGUID = str(uuid.uuid4()) #If no ExecutionGUID is passed in, generate a new GUID

        LogEntry = {
            'ExecutionGUID': ExecutionGUID
            , 'ParentExecutionGUID': VariedParameters.get('ParentExecutionGUID')
            , 'Begin': Begin
            , 'End': End
            , 'Severity': VariedParameters.get('Severity')
            , 'Caller': Caller
            , 'CallStack': CallStack
            , 'Action': VariedParameters.get('Action')
            , 'RowCount': VariedParameters.get('RowCount')
            , 'Source': VariedParameters.get('Source')
            , 'Target': VariedParameters.get('Target')
            , 'Result': VariedParameters.get('Result')
            , 'File': VariedParameters.get('File')
            , 'Parameters': Parameters
        }

        LogEntries.append(LogEntry) #Add the log entry to the collection of log entries for the current run

    except Exception as e:
        #Report the error
        print('Error in LogStep on line', e.__traceback__.tb_lineno, ':', str(e))

def Main():
    #Variable(s) defined outside of this function, but set within this function
    global FullPath_Archive
    global FullPath_Bronze
    global FullPath_Configurations
    global FullPath_Configurations_Column
    global FullPath_Configurations_File
    global FullPath_Inbound
    global FullPath_LogFile

    #Set script-wide scalar variables
    Begin = datetime.now()
    CallStack = r'Main'
    Continue = True
    EmptyFolder = ''
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    File = ''
    IsValid_LogFile = False
    Parameters = {
        'ColumnConfigurationFilename': ColumnConfigurationFilename
        , 'FileConfigurationFilename': FileConfigurationFilename
        , 'FullPath_Root': FullPath_Root
        , 'InboundSourceFolder': InboundSourceFolder
    }
    Result = Result_Success

    try:
        #Set script-wide calculated variables - included in the "try/except" block in case an expression fails
        FullPath_Archive = os.path.join(FullPath_Root, 'Archive')
        FullPath_Bronze = os.path.join(FullPath_Root, 'Bronze')
        FullPath_Configurations = os.path.join(FullPath_Root, 'Admin')
        FullPath_Configurations_Column = os.path.join(FullPath_Configurations, ColumnConfigurationFilename)
        FullPath_Configurations_File = os.path.join(FullPath_Configurations, FileConfigurationFilename)
        FullPath_Inbound = os.path.join(FullPath_Root, 'Inbound')
        FullPath_LogFile = os.path.join(FullPath_Root, 'Admin', 'Log.txt')
        if(InboundSourceFolder == ''): AllInboundFolders = True #If no source folder was specified
        elif(FullPath_Root not in InboundSourceFolder): AllInboundFolders = False #If source folder was specified, but not the full path (as expected)

        #Validate the log file first so that all subsequent steps & errors can be properly logged
        Result = ValidateLogFile(CallStack, ExecutionGUID)
        if(Result != Result_Success): raise Exception('Invalid Log File: ' + FullPath_LogFile + '\n' + Result) #Report the error and don't continue - can't log the error because log file is invalid

        #Mark the log file as valid only after it's passed all validation; otherwise, consider the log file invalid
        IsValid_LogFile = True

        Result = Result_Success

    except Exception as e:
        IsValid_LogFile = False
        #Report the error
        print('Error in Main on line', e.__traceback__.tb_lineno, ':', str(e))

    try:
        #Validate root level parameters
        if(Result == Result_Success): Result = ValidateRootParameters(CallStack, ExecutionGUID)
        if(Result != Result_Success): raise Exception('Error in ValidateRootParameters') #Log the error and don't continue

        #Once all parameters are valid, process all appropriate inbound files
        if Continue:
            #Loop through the Inbound folder & sub-folder(s) looking for files to ingest
            RootInboundFolder = os.path.join(FullPath_Root, 'Inbound')
            if AllInboundFolders: #Process files in all Inbound sub-folders
                for FolderName in os.listdir(RootInboundFolder):
                    #Loop through all files in the current Inbound sub-folder
                    InboundFolder = os.path.join(RootInboundFolder, FolderName)
                    Result = ProcessInboundFolder(CallStack, InboundFolder, ExecutionGUID)
            else: #Process files in just the specified Inbound sub-folder
                #Loop through all files in the specified Inbound sub-folder
                InboundFolder = os.path.join(RootInboundFolder, InboundSourceFolder)
                Result = ProcessInboundFolder(CallStack, InboundFolder, ExecutionGUID)

        if not InboundFileFound: Result = Result_Success

        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, Result = Result, Severity = Severity_Info)

    except Exception as e:
        #Return the error
        Result = str(e)

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, File = File, Result = str(e), Severity = Severity_Error)

        #Report the error
        print('Error in Main on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        if IsValid_LogFile: WriteToLogFile() #After everything has finished, even if an error occurred, log to the log file only if the log file was found valid

def MoveFile(CallStack, FullPath_SourceFile, FullPath_TargetFile, ParentExecutionGUID):
    Begin = datetime.now()
    CallStack = CallStack + ' > MoveFile' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'FullPath_SourceFile': FullPath_SourceFile
        , 'FullPath_TargetFile': FullPath_TargetFile
        , 'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Result_Success
    try:
        os.rename(FullPath_SourceFile, FullPath_TargetFile)

        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)

    except Exception as e:
        #Return the error
        Result = str(e)

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

        #Report the error
        print('Error in MoveFile on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result

def ProcessFile(CallStack, FileName, FullPath_Configurations_Column, FullPath_Configurations_File, InboundFolder, ParentExecutionGUID):
    Begin = datetime.now()
    InboundFile = ''
    CallStack = CallStack + ' > ProcessFile' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'FileName': FileName
        , 'FullPath_Configurations_Column': FullPath_Configurations_Column
        , 'FullPath_Configurations_File': FullPath_Configurations_File
        , 'InboundFolder': InboundFolder
        , 'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Result_Success
    try:
        #Process only .csv or .txt files
        if FileName.endswith('.csv') or FileName.endswith('.txt'):
            InboundFile = os.path.join(InboundFolder, FileName) #Generate the full path and file name of the file

            #Reset ConfigurationFileID for each file
            ConfigurationFileID = 0 #Make sure the values from previous file(s) aren't used

            #Parse the folder path of the current file to determine the "source" folder of the current file to help filter for the correct configurations
            Source = InboundFolder.replace(FullPath_Inbound, '') #Get only the last folder name from the path
            Source = Source.replace('\\', '') #Remove all "\"

            #Get all file level configurations for the current source
            Result = RetrieveConfigurations_File(CallStack, FullPath_Configurations_File, ParentExecutionGUID, Source)
            if(Result != Result_Success): raise Exception('Error in ValidateColumnHeader') #Log the error and don't continue

            #Get ConfigurationID from the file level configurations for the current source
            ConfigurationFileID = Configurations_File_CurrentFile[['ConfigurationFileID']].iloc[0,0] #Use .iloc[0,0] to pinpoint the exact cell and exclude the column header in the return value

            #Get Delimiter of current file from file level configurations
            ExpectedDelimiter = Configurations_File_CurrentFile[['Delimiter']].iloc[0,0]

            #Get all column level configurations for the current ConfigurationFileID
            Result = RetrieveConfigurations_Column(CallStack, ConfigurationFileID, FullPath_Configurations_Column, ParentExecutionGUID)
            if(Result != Result_Success): raise Exception('Error in ValidateColumnHeader') #Log the error and don't continue

            #Read the current file into a dataframe
            CurrentFile = pd.read_csv(InboundFile, delimiter = ExpectedDelimiter)

            #Put actual column headers into a list
            ActualColumnsAsList = CurrentFile.columns.tolist()

            #Put expected column headers into a list
            ExpectedColumnsAsList = Configurations_Column_CurrentFile['ColumnName_File'].values.tolist() #Use the values in the column [ColumnName_File] in the column level configurations

            #Validate the actual column headers
            Result = ValidateColumnHeader(ActualColumnsAsList, CallStack, ExpectedColumnsAsList, ParentExecutionGUID)
            if(Result != Result_Success): raise Exception('Error in ValidateColumnHeader') #Log the error and don't continue

            #Load the file to the Bronze layer
            Result = CopyToBronze(CallStack, Configurations_Column_CurrentFile, ParentExecutionGUID, Source, CurrentFile)                
            if(Result != Result_Success): raise Exception('Error in CopyToBronze') #Log the error and don't continue

            #Archive the file
            FullPath_TargetFile = os.path.join(FullPath_Archive, Source, FileName)
            Result = MoveFile(CallStack, InboundFile, FullPath_TargetFile, ParentExecutionGUID)
            if(Result != Result_Success): raise Exception('Error in MoveFile') #Log the error and don't continue

        InboundFile = '' #Make sure the values from previous file(s) aren't used

        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, File = InboundFile, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)

    except Exception as e:
        #Return the error
        Result = str(e)

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, File = InboundFile, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

        #Report the error
        print('Error in ProcessFile on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result

def ProcessInboundFolder(CallStack, InboundFolder, ParentExecutionGUID):
    #Variable(s) defined outside of this function, but set within this function
    global InboundFileFound

    Begin = datetime.now()
    CallStack = CallStack + ' > ProcessInboundFolder' #Add the current function to the call stack
    EmptyFolder = ''
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    InboundFileFound = False
    Parameters = {
        'InboundFolder': InboundFolder
        , 'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Result_Success
    try:
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
            for FileName in os.listdir(InboundFolder):
                File = FileName #Capture the current file name for logging
                ProcessFile(CallStack, FileName, FullPath_Configurations_Column, FullPath_Configurations_File, InboundFolder, ExecutionGUID)

        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)

    except Exception as e:
        #Return the error
        Result = str(e)

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

        #Report the error
        print('Error in ProcessInboundFolder on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result

def RetrieveConfigurations_Column(CallStack, ConfigurationFileID, FullPath_Configurations_Column, ParentExecutionGUID):
    #Variable(s) defined outside of this function, but set within this function
    global Configurations_Column_CurrentFile

    #Get all column level configurations for the specified ConfigurationFileID
    Begin = datetime.now()
    CallStack = CallStack + ' > RetrieveConfigurations_Column' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'ConfigurationFileID': str(ConfigurationFileID)
        , 'FullPath_Configurations_Column': FullPath_Configurations_Column
        , 'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Result_Success
    try:
        #Retrieve all column level configurations for the specified ConfigurationFileID
        Configurations_Column_All = pd.read_csv(FullPath_Configurations_Column, delimiter=DelimiterDefault)

        #Filter for the value in the column [ConfigurationFileID] - this will not work properly if there are multiple records in the configuration file for the specified ConfigurationFileID
        Configurations_Column_CurrentFile = Configurations_Column_All[Configurations_Column_All['ConfigurationFileID'] == int(ConfigurationFileID)]

        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)

    except Exception as e:
        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

        #Report the error
        print('Error in RetrieveConfigurations_Column on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result

def RetrieveConfigurations_File(CallStack, FullPath_Configurations_File, ParentExecutionGUID, Source):
    #Variable(s) defined outside of this function, but set within this function
    global Configurations_File_CurrentFile

    #Get all file level configurations for the current source
    Begin = datetime.now()
    CallStack = CallStack + ' > RetrieveConfigurations_File' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'FullPath_Configurations_File': FullPath_Configurations_File
        , 'ParentExecutionGUID': ParentExecutionGUID
        , 'Source': Source
    }
    Result = Result_Success
    try:
        #Retrieve all file level configurations for the specified source
        Configurations_File_All = pd.read_csv(FullPath_Configurations_File, delimiter=DelimiterDefault)

        #Filter for the values in the column [Source]
        Configurations_File_CurrentFile = Configurations_File_All[Configurations_File_All['Source'] == Source]
        
        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)
        
    except Exception as e:
        #Return the error
        Result = str(e)

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

        #Report the error
        print('Error in RetrieveConfigurations_File on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result

def ValidateColumnHeader(ActualColumnsAsList, CallStack, ExpectedColumnsAsList, ParentExecutionGUID):
    Begin = datetime.now()
    CallStack = CallStack + ' > ValidateColumnHeader' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'ActualColumnsAsList': ActualColumnsAsList
        , 'ExpectedColumnsAsList': ExpectedColumnsAsList
        , 'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Result_Success
    try:
        #Determine extra columns in actual
        ExtraColumns = [item for item in ActualColumnsAsList if item not in ExpectedColumnsAsList]

        #Determine columns missing from actual
        MissingColumns = [item for item in ExpectedColumnsAsList if item not in ActualColumnsAsList]
        
        #Report extra & missing columns
        if(len(ExtraColumns) > 0):
            Result = 'Extra column(s) found: ' + str(ExtraColumns)
            LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Error) #Log the error & continue
        if(len(MissingColumns) > 0):
            Result = 'Column(s) missing: ' + str(MissingColumns)
            LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Error) #Log the error & continue
        if(len(ExtraColumns) + len(MissingColumns) == 0):
            #Log the step
            LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)
        
    except Exception as e:
        #Return the error
        Result = str(e)

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

        #Report the error
        print('Error in ValidateColumnHeader on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result

def ValidateLogFile(CallStack, ParentExecutionGUID):
    Begin = datetime.now()
    CallStack = CallStack + ' > ValidateLogFile' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Result_Success
    try:
        #Validate the log file existence; create it if it's not found
        if(FullPath_LogFile != '') and not os.path.exists(FullPath_LogFile):
            os.mkdir(FullPath_LogFile) #Create an empty file
            with open(FullPath_LogFile, 'w') as f: f.write(LogFileDefinition) #Add the correct column headers
            Result = 'Log file was not found and therefore created: ' + FullPath_LogFile #Return the warning but continue and don't fail

        #Put actual column headers into a list
        LogFile = pd.read_csv(FullPath_LogFile, delimiter = DelimiterDefault)
        ActualColumnsAsList = LogFile.columns.tolist()

        #Validate the actual column headers
        Result = ValidateColumnHeader(ActualColumnsAsList, CallStack, LogFileDefinition, ParentExecutionGUID)

        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, File = FullPath_LogFile, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)

    except Exception as e:
        #Return the error
        Result = str(e)

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

        #Report the error
        print('Error in ValidateLogFile on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result

def ValidateRootParameters(CallStack, ParentExecutionGUID):
    Begin = datetime.now()
    CallStack = CallStack + ' > ValidateRootParameters' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'ColumnConfigurationFilename': ColumnConfigurationFilename
        , 'FileConfigurationFilename': FileConfigurationFilename
        , 'FullPath_Root': FullPath_Root
        , 'InboundSourceFolder': InboundSourceFolder
        , 'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Result_Success
    try:
        #Validate that each required parameter has a value; don't explicitly raise any excpetion if an issue is found, just log it and move on to the next parameter
        if(ColumnConfigurationFilename == ''):
            Continue = False
            LogStep(Begin, CallStack, ExecutionGUID, Parameters, Result = 'Script parameter value missing: ColumnConfigurationFilename', Severity = Severity_Error)
        if(FileConfigurationFilename == ''):
            Continue = False
            LogStep(Begin, CallStack, ExecutionGUID, Parameters, Result = 'Script parameter value missing: FileConfigurationFilename', Severity = Severity_Error)
        if(FullPath_Root == ''):
            Continue = False
            LogStep(Begin, CallStack, ExecutionGUID, Parameters, Result = 'Script parameter value missing: FullPath_Root', Severity = Severity_Error)

        #Validate folders
        if(FullPath_Root != '') and not os.path.exists(FullPath_Root):
            os.mkdir(FullPath_Root) #If a value was provided but doesn't exist, create it
            Result = 'Folder path was not found and therefore created: ' + FullPath_Root #Return the warning but continue and don't fail
        if(InboundSourceFolder != '') and not os.path.exists(InboundSourceFolder):
            os.mkdir(InboundSourceFolder) #If a value was provided but doesn't exist, create it
            Result = 'Folder path was not found and therefore created: ' + InboundSourceFolder #Return the warning but continue and don't fail
        
        #Validate configuration files
        if(FullPath_Configurations != '') and not os.path.exists(FullPath_Configurations): raise Exception('Error in FullPath_Configurations') #Log the error
        
    except Exception as e:
        #Return the error
        Result = str(e)

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

        #Report the error
        print('Error in ValidateRootParameters on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result

def WriteToLogFile():
    #Don't log the results of this function
    try:
        EntriesToLog = pd.DataFrame(LogEntries)
        EntriesToLog = EntriesToLog.sort_values(by = 'Begin') #Explicitly order the entries by when they occurred, otherwise they'll be ordered by when they were logged, which means the first function called would be logged last
        EntriesToLog.to_csv(FullPath_LogFile, sep = DelimiterDefault, header = False, index = False, mode = 'a')
    except Exception as e:
        #Report the error
        print('Error in WriteToLogFile on line', e.__traceback__.tb_lineno, ':', str(e))

#****************************************************************************************
#ENTRY
#****************************************************************************************
print('***********************************************************************************') #Delineate manual runs
if __name__ == '__main__':
    Main()

#****************************************************************************************
#DONE
#****************************************************************************************
print(datetime.now(), ': Done')
