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
import os
import pandas as pd
import uuid
from datetime import datetime
from pathlib import Path

#****************************************************************************************
#PARAMETERS
#   These need to be set by whatever calls this script
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
ConfigurationFileID = 0
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
IsValid_LogFile = False
LogEntries = []
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
        print('Error in', Caller, '> CopyToBronze on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result

def LogStep(Begin, CallStack, ExecutionGUID, Parameters, **VariedParameters): #The explicit parameters are required; anything passed into **VariedParameters is optional; different parameters may be passed into **VariedParameters
    #Variable(s) defined outside of this function, but set within this function
    global LogEntries

    LogEntries = Utilities.LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, **VariedParameters)

def Main():
    #Variable(s) defined outside of this function, but set within this function
    global FullPath_Archive
    global FullPath_Bronze
    global FullPath_Configurations
    global FullPath_Configurations_Column
    global FullPath_Configurations_File
    global FullPath_Inbound
    global FullPath_LogFile
    global LogEntries

    #Set script-wide scalar variables
    Begin = datetime.now()
    CallStack = r'Main'
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
        if(InboundSourceFolder == ''): AllInboundFolders = True #If no source folder was specified
        elif(FullPath_Root not in InboundSourceFolder): AllInboundFolders = False #If source folder was specified, but not the full path (as expected)

        #Validate root level parameters
        Result = ValidateRootParameters(CallStack, ExecutionGUID)
        if(Result != Result_Success): raise Exception('Error in ValidateRootParameters') #Log the error and don't continue

        #Validate the log file so that all subsequent steps & errors can be properly logged
        Result, FullPath_LogFile, IsValid_LogFile, LogEntries = Utilities.ValidateLogFile(CallStack, FullPath_Root, LogEntries, ExecutionGUID)
        if(Result != Result_Success): raise Exception(Result) #Report the error and don't continue - can't log the error because log file is invalid

        #Mark the log file as valid only after it's passed all validation; otherwise, consider the log file invalid
        IsValid_LogFile = True

        Result = Result_Success

    except Exception as e:
        #The log file is invalid
        IsValid_LogFile = False

        #Report the error
        print('Error in', Caller, '> Main on line', e.__traceback__.tb_lineno, ':', str(e))

    try:
        if IsValid_LogFile:
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
        print('Error in', Caller, '> Main on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        if IsValid_LogFile: Utilities.WriteToLogFile(DelimiterDefault, FullPath_LogFile, LogEntries)

def ProcessFile(CallStack, FileName, FullPath_Configurations_Column, FullPath_Configurations_File, InboundFolder, ParentExecutionGUID):
    #Variable(s) defined outside of this function, but set within this function
    global ConfigurationFileID
    global Configurations_Column_CurrentFile
    global Configurations_File_CurrentFile
    global LogEntries

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
            Result, Configurations_File_CurrentFile, LogEntries = Utilities.RetrieveConfigurations_File(CallStack, FullPath_Configurations_File, LogEntries, ParentExecutionGUID, Source)
            if(Result != Result_Success): raise Exception('Error in ValidateColumnHeader') #Log the error and don't continue

            #Get ConfigurationID from the file level configurations for the current source
            ConfigurationFileID = Configurations_File_CurrentFile[['ConfigurationFileID']].iloc[0,0] #Use .iloc[0,0] to pinpoint the exact cell and exclude the column header in the return value

            #Get Delimiter of current file from file level configurations
            ExpectedDelimiter = Configurations_File_CurrentFile[['Delimiter']].iloc[0,0]

            #Get all column level configurations for the current ConfigurationFileID
            Result, Configurations_Column_CurrentFile, LogEntries = Utilities.RetrieveConfigurations_Column(CallStack, ConfigurationFileID, FullPath_Configurations_Column, LogEntries, ParentExecutionGUID)
            if(Result != Result_Success): raise Exception('Error in RetrieveConfigurations_Column') #Log the error and don't continue

            #Read the current file into a dataframe
            CurrentFile = pd.read_csv(InboundFile, delimiter = ExpectedDelimiter)

            #Put actual column headers into a list
            ActualColumnsAsList = CurrentFile.columns.tolist()

            #Put expected column headers into a list
            ExpectedColumnsAsList = Configurations_Column_CurrentFile['ColumnName_File'].values.tolist() #Use the values in the column [ColumnName_File] in the column level configurations

            #Validate the actual column headers
            Result, LogEntries = Utilities.ValidateColumnHeader(ActualColumnsAsList, CallStack, ExpectedColumnsAsList, LogEntries, ParentExecutionGUID)
            if(Result != Result_Success): raise Exception('Error in ValidateColumnHeader') #Log the error and don't continue

            #Load the file to the Bronze layer
            Result = CopyToBronze(CallStack, Configurations_Column_CurrentFile, ParentExecutionGUID, Source, CurrentFile)                
            if(Result != Result_Success): raise Exception('Error in CopyToBronze') #Log the error and don't continue

            #Archive the file
            FullPath_TargetFile = os.path.join(FullPath_Archive, Source, FileName)
            Result, LogEntries = Utilities.MoveFile(CallStack, InboundFile, FullPath_TargetFile, LogEntries, ParentExecutionGUID)
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
        print('Error in', Caller, '> ProcessFile on line', e.__traceback__.tb_lineno, ':', str(e))

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
        print('Error in', Caller, '> ProcessInboundFolder on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result

def ValidateRootParameters(CallStack, ParentExecutionGUID):
    Begin = datetime.now()
    CallStack = CallStack + ' > ValidateRootParameters' #Add the current function to the call stack
    Continue = True
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

        if Continue:
            #Validate folders
            if(FullPath_Root != '') and not os.path.exists(FullPath_Root):
                os.mkdir(FullPath_Root) #If a value was provided but doesn't exist, create it
                Result = 'Folder path was not found and therefore created: ' + FullPath_Root #Return the warning but continue and don't fail
            if(InboundSourceFolder != '') and not os.path.exists(InboundSourceFolder):
                os.mkdir(InboundSourceFolder) #If a value was provided but doesn't exist, create it
                Result = 'Folder path was not found and therefore created: ' + InboundSourceFolder #Return the warning but continue and don't fail
       
            #Validate configuration files
            if(FullPath_Configurations != '') and not os.path.exists(FullPath_Configurations): raise Exception('Error in FullPath_Configurations') #Log the error

            #Log the step
            LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)

    except Exception as e:
        #Return the error
        Result = str(e)

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

        #Report the error
        print('Error in', Caller, '> ValidateRootParameters on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result

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
