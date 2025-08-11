""""
DESCRIPTION
    Load all valid files into the Bronze layer

PARAMETERS
    InboundSourceFolder
        - Optional
        - The name of a single folder in /<Root>/Bronze/Inbound/
        - If used, only files in this folder will be processed
        - If not used, all folders in /<Root>/Bronze/Inbound/ will be processed
    ParentExecutionGUID
        - Optional
        - The GUID of the parent script that called this script
        - If used, this will be used to link the log entries of this script to the parent script
        - If not used, a new GUID will be generated for this script

STEPS
1. Validate all required folders
    a. Create them if they don't already exist
2. Validate Log file
    a. If not found in /<Root>/Admin/:
        1. Create Log file in /<Root>/Admin/ with definition stored in Utilities.py
    b. If found:
        1. Validate column header
            a. If actual column header doesn't match expected column header stored in Utilities.py:
                1. Create Log file in /<Root>/Admin/ with definition stored in Utilities.py
3. Validate Root Parameters
    a. Make sure all required parameters have a value
    b. Make sure all provided values are valid
4. Validate configuration files
    a. Validate file level configuration file
        1. If file doesn't exist, create it with definition stored in Utilities.py
        2. If file exists
            a. Validate column header
                1. If actual column header doesn't match expected column header stored in Utilities.py:
                    a. Log error
                    b. Fail entire script
                2. If actual column header matches expected column header:
                    a. load entire file into memory
    b. Validate column level configuration file
        1. If file doesn't exist, create it with definition stored in Utilities.py
        2. If file exists
            a. Validate column header
                1. If actual column header doesn't match expected column header stored in Utilities.py:
                    a. Log error
                    b. Fail entire script
                2. If actual column header matches expected column header:
                    a. load entire file into memory
5. Loop through all /<Root>/Bronze/Inbound/<Source>/ folders
    a. Validate configuration files for specific <Source>
        1. If no records are found in in-memory file or column level configurations for specific <Source>
            a. Log error
            b. Do NOTHING with files in current /<Root>/Bronze/Inbound/<Source>/ folder
            c. Move to next /<Root>/Bronze/Inbound/<Source>/ folder
    b. For each file in current /<Root>/Bronze/Inbound/<Source>/ folder:
        1. If actual column header doesn't match expected column header
            a. Rename file to reflect issue
            b. Move file to appropriate /<Root>/Bronze/Inbound/<Source>/Error/ folder
        2. If actual column header matches expected column header
            a. Move file to appropriate /<Root>/Silver/Inbound/<Source>/
"""

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
FullPath_Bronze_Error_CurrentSource = ''
#FullPath_Bronze_Inbound_CurrentSource = ''
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
    FullPath_Bronze_Inbound_Source = ''
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

    except Exception as e:
        print(Utilities.BuildErrorMessage(CurrentFunction, CurrentScriptFile, e, False, Parameters)) #Report the error

    if(Utilities.IsValid_LogFile):
        #Now process all Inbound folders, along with all necessary validations
        try:
            if AllInboundFolders: #Process all Inbound sub-folders
                for FolderName in os.listdir(Utilities.FullPath_Bronze_Inbound):
                    InboundFolder = os.path.join(Utilities.FullPath_Bronze_Inbound, FolderName)
                    Result = ProcessInboundFolder(CurrentFunction, InboundFolder, ExecutionGUID)
            else: #Process just the specified Inbound sub-folder
                InboundFolder = os.path.join(Utilities.FullPath_Bronze_Inbound, InboundSourceFolder)
                Result = ProcessInboundFolder(CurrentFunction, InboundFolder, ExecutionGUID)

            if not InboundFileFound: Result = Utilities.Result_Success #If no Inbound files were found, set the result to success so that the script doesn't fail; this is not an error, just a condition

            #Log the step
            LogStep(Begin, CurrentFunction, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Utilities.Severity_Info)

        except Exception as e:
            Result = Utilities.BuildErrorMessage(CurrentFunction, CurrentScriptFile, e, True) #Build the error message
            LogStep(Begin, CurrentFunction, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Utilities.Severity_Error) #Log the error
            print(Result) #Report the error

        finally:
            Utilities.WriteToLogFile(CurrentFunction, LogEntries, ExecutionGUID)

def ProcessInboundFile(CallStack, InboundFile, ParentExecutionGUID, Source):
    #Variable(s) defined outside of this function, but set within this function
    global Bronze
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

            if (CurrentFile.empty): #Do not continue only if the current file is empty
                #Rename the file to indicate that it is empty
                FileName = FileName.replace(FileExtension, '') + '.Empty' + FileExtension
            else:
                #Add "standard" columns
                CurrentFile['ExecutionGUID'] = ExecutionGUID #Add the ExecutionGUID
                CurrentFile['IngestDatetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') #Add the current date/time to the IngestDatetime column
                CurrentFile['Source'] = Source #Add the Inbound source folder
                CurrentFile['SourceFile'] = FileName #Add the file name of the current file
                #Put actual column headers into a list
                ActualColumnsAsList = CurrentFile.columns.tolist()

                #Put expected column headers into a list
                ExpectedColumnsAsList = Configurations_Column_CurrentFile['ColumnName_File'].values.tolist() #Use the values in the column [ColumnName_File] in the column level configurations
                ExpectedColumnsAsList = [x for x in ExpectedColumnsAsList if pd.notnull(x)] #Remove any null values from the list

                #Validate the actual column header
                Issue = ''
                LogEntries, Result, Issue = Utilities.ValidateColumnHeader(ActualColumnsAsList, CallStack, ExpectedColumnsAsList, LogEntries, ParentExecutionGUID)
                if(Result != Utilities.Result_Success): #There was an issue validating the column header
                    #Rename the file to indicate the issue found
                    FileName = FileName.replace(FileExtension, '') + '.InvalidColumnHeader.' + Issue + FileExtension #Change the file name to indicate that it has an invalid column header
                    FullPath_Error_CurrentFile = os.path.join(FullPath_Bronze_Error_CurrentSource, FileName) #Set the full path of the error file
                   
                    #Move the file to the appropriate Error folder                    
                    LogEntries, Result = Utilities.MoveFile(CallStack, InboundFile, FullPath_Error_CurrentFile, LogEntries, ParentExecutionGUID)

        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, File = InboundFile, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Utilities.Severity_Info)

    except Exception as e:
        Result = Utilities.BuildErrorMessage(CurrentFunction, CurrentScriptFile, e, True) #Build the error message
        LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Utilities.Severity_Error) #Log the error
        print(Result) #Report the error

    finally:
        #Return the result
        return Result

def ProcessInboundFolder(CallStack, FullPath_Bronze_Inbound_CurrentSource, ParentExecutionGUID, Source):
    #Variable(s) defined outside of this function, but set within this function
    global ConfigurationFileID
    global Configurations_Column_CurrentFile
    global Configurations_File_CurrentFile
    global ExpectedDelimiter
    global FullPath_Bronze_Error_CurrentSource
    global FullPath_Silver_Inbound_CurrentSource
    global InboundFileFound
    global LogEntries

    #Local variables
    Begin = datetime.now()
    CurrentFunction = 'ProcessInboundFolder'
    CallStack = f'{CallStack} > {CurrentFunction}' #Add the current function to the call stack
    EmptyFolder = ''
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    InboundFileFound = False
    Parameters = {
        'FullPath_Bronze_Inbound_CurrentSource': FullPath_Bronze_Inbound_CurrentSource
        , 'ParentExecutionGUID': ParentExecutionGUID
        , 'Source': Source
    }
    Result = Utilities.Result_Success
    try:
        #Loop through all files in the current Inbound sub-folder
        if not os.listdir(FullPath_Bronze_Inbound_CurrentSource):
            #There are no files in the folder
            InboundFileFound = False
            if(EmptyFolder != ''): EmptyFolder = EmptyFolder + ', '
            EmptyFolder = EmptyFolder + FullPath_Bronze_Inbound_CurrentSource
            Result = 'No files were found for processing in ' + EmptyFolder
        else:
            #There are files in the folder
            InboundFileFound = True

            #Set & validate the full paths of subfolders for the current source
            LogEntries, Result, FullPath_Bronze_Error_CurrentSource = Utilities.BuildFolderPath(CallStack, Utilities.FullPath_Bronze_Error, Source, LogEntries, ParentExecutionGUID)
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
                    #Ingest each file in the current Inbound folder
                    for FileName in os.listdir(Source):
                        InboundFile = os.path.join(FullPath_Bronze_Inbound_CurrentSource, Source, FileName) #Generate the full path and file name of the file
                        Result = ProcessInboundFile(CallStack, InboundFile, ExecutionGUID, Source)
                        if(Result != Utilities.Result_Success): raise Exception('Error in ProcessInboundFile') #Log the error and don't continue

                        #Move the file to the appropriate Silver Inbound folder
                        FullPath_TargetFile = os.path.join(FullPath_Silver_Inbound_CurrentSource, FileName)
                        Result, LogEntries = Utilities.MoveFile(CallStack, InboundFile, FullPath_TargetFile, LogEntries, ParentExecutionGUID)
                        if(Result != Utilities.Result_Success): raise Exception('Error in MoveFile') #Log the error and don't continue

        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Utilities.Severity_Info)

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
print('***********************************************************************************') #Delineate manual runs
if __name__ == '__main__':
    Main()

#****************************************************************************************
#DONE
#****************************************************************************************
print(datetime.now(), ': Done')
