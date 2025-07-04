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

#JUST FOR TESTING
print('***********************************************************************************')
print(datetime.now())

#****************************************************************************************
#PARAMETERS
#****************************************************************************************
ConfigurationColumn = r'ConfigurationColumn.txt' #Expected: the file name of the file containing column level configurations, excluding path
ConfigurationFile = r'ConfigurationFile.txt' #Expected: the file name of the file containing file level configurations, excluding path
FullPath_Root = r'' #Expected: the full path of the root folder for all files & code
FullPath_Source = r'' #Expected: either empty string, nothing, or a valid folder name in the root bronze folder

#****************************************************************************************
#FUNCTIONS
#****************************************************************************************
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
#        print('LogEntry: ', LogEntry)
        LogEntries.append(LogEntry) #Add the log entry to the collection of log entries for the current run
    except Exception as e:
        #Report the error
        print('Error in LogStep: ', str(e))

def Main():
    Begin = datetime.now()
    CallStack = r'Main'
    Continue = True
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    File = ''
    Parameters = ''
    Result = Result_Success
    try:
        #Validate that each required parameter has a value
        if(ConfigurationColumn == ''):
            Continue = False
            LogStep(Begin, CallStack, ExecutionGUID, Parameters, Result = 'Script parameter value missing: ConfigurationColumn', Severity = Severity_Error)
        if(ConfigurationFile == ''):
            Continue = False
            LogStep(Begin, CallStack, ExecutionGUID, Parameters, Result = 'Script parameter value missing: ConfigurationFile', Severity = Severity_Error)
        if(FullPath_Root == ''):
            Continue = False
            LogStep(Begin, CallStack, ExecutionGUID, Parameters, Result = 'Script parameter value missing: FullPath_Root', Severity = Severity_Error)

        #Once all parameters are valid, process all appropriate inbound files
        if Continue:
            #Loop through Inbound folder looking for files to ingest
            RootInboundFolder = os.path.join(FullPath_Root, 'Inbound')
            if AllInboundFolders: #Process files in all Inbound sub-folders
                for FolderName in os.listdir(RootInboundFolder):
                    InboundFolder = os.path.join(RootInboundFolder, FolderName)
                    #Loop through all files in the current Inbound sub-folder
                    for FileName in os.listdir(InboundFolder):
                        File = FileName #Capture the current file name for logging
                        ProcessFile(CallStack, DelimiterDefault, FileName, FullPath_Configurations_Column, FullPath_Configurations_File, InboundFolder, ExecutionGUID)
            else: #Process files in just the specified Inbound sub-folder
                InboundFolder = os.path.join(RootInboundFolder, FullPath_Source)
                #Loop through all files in the specified Inbound sub-folder
                for FileName in os.listdir(InboundFolder):
                    File = FileName #Capture the current file name for logging
                    ProcessFile(CallStack, DelimiterDefault, FileName, FullPath_Configurations_Column, FullPath_Configurations_File, InboundFolder, ExecutionGUID)
        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, Result = Result, Severity = Severity_Info)
    except Exception as e:
        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, File = File, Result = str(e), Severity = Severity_Error)
    finally:
        WriteToLogFile()

def ProcessFile(CallStack, DelimiterDefault, FileName, FullPath_Configurations_Column, FullPath_Configurations_File, InboundFolder, ParentExecutionGUID):
    Begin = datetime.now()
    InboundFile = ''
    CallStack = CallStack + ' > ProcessFile' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'DelimiterDefault': DelimiterDefault
        , 'FileName': FileName
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
            Configurations_File_CurrentFile = RetrieveConfigurations_File(CallStack, FullPath_Configurations_File, ParentExecutionGUID, Source)
            #Get ConfigurationID from the file level configurations for the current source
            ConfigurationFileID = Configurations_File_CurrentFile[['ConfigurationFileID']].iloc[0,0] #Use .iloc[0,0] to pinpoint the exact cell and exclude the column header in the return value
            #Get Delimiter of current file from file level configurations
            ExpectedDelimiter = Configurations_File_CurrentFile[['Delimiter']].iloc[0,0]
            #Get all column level configurations for the current ConfigurationFileID
            Configurations_Column_CurrentFile = RetrieveConfigurations_Column(CallStack, ConfigurationFileID, FullPath_Configurations_Column, ParentExecutionGUID)
            #Read the current file into a dataframe
            CurrentFile = pd.read_csv(InboundFile, delimiter = ExpectedDelimiter)
            #Put expected column headers into a list
            ActualColumnsAsList = CurrentFile.columns.tolist()
            #Put actual column headers into a list
            ExpectedColumnsAsList = Configurations_Column_CurrentFile['ColumnName_File'].values.tolist() #Use the values in the column [ColumnName_File] in the column level configurations
            #Validate the actual column headers
            Result = ValidateColumnHeader(ActualColumnsAsList, CallStack, ExpectedColumnsAsList, ParentExecutionGUID)
            #Handle any issues
            if(Result != Result_Success):
                #Log the error and don't continue
                raise Exception('Error in ValidateColumnHeader')
        InboundFile = '' #Make sure the values from previous file(s) aren't used
        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, File = InboundFile, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)
    except Exception as e:
        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, File = InboundFile, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

def RetrieveConfigurations_Column(CallStack, ConfigurationFileID, FullPath_Configurations_Column, ParentExecutionGUID):
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
        #Return the result
        return Configurations_Column_CurrentFile
    except Exception as e:
        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

def RetrieveConfigurations_File(CallStack, FullPath_Configurations_File, ParentExecutionGUID, Source):
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
        #Return the result
        return Configurations_File_CurrentFile
    except Exception as e:
        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

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
            #Log the error & continue
            LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Error)
        if(len(MissingColumns) > 0):
            Result = 'Column(s) missing: ' + str(MissingColumns)
            #Log the error & continue
            LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Error)
        if(len(ExtraColumns) + len(MissingColumns) == 0):
            #Log the step
            LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)
        #Return the result
        return Result
    except Exception as e:
        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

def WriteToLogFile():
    #Don't log this function
    try:
        EntriesToLog = pd.DataFrame(LogEntries)
        EntriesToLog = EntriesToLog.sort_values(by = 'Begin') #Explicitly order the entries by when they occurred, otherwise they'll be ordered by when they were logged, which means the first function called would be logged last
        EntriesToLog.to_csv(LogFile, sep = DelimiterDefault, header = False, index = False, mode = 'a')
    except Exception as e:
        #Report the error
        print('Error in WriteToLogFile: ', str(e))

#****************************************************************************************
#LOCAL VARIABLES
#****************************************************************************************
AllInboundFolders = False
Caller = os.path.realpath(__file__)
Continue = True
DelimiterDefault = r'|'
FullPath_Bronze = os.path.join(FullPath_Root, 'Bronze')
FullPath_Configurations = os.path.join(FullPath_Root, 'Admin')
FullPath_Configurations_File = os.path.join(FullPath_Configurations, ConfigurationFile)
FullPath_Configurations_Column = os.path.join(FullPath_Configurations, ConfigurationColumn)
FullPath_Inbound = os.path.join(FullPath_Root, 'Inbound')
LogEntries = []
LogFile = os.path.join(FullPath_Root, 'Admin', 'Log.txt')
Result_Success = r'Success'
Severity_Error = r'Error'
Severity_Info = r'Info'
if(FullPath_Source == ''): AllInboundFolders = True #If no source folder was specified
elif(FullPath_Root not in FullPath_Source): AllInboundFolders = False #If source folder was specified, but not the full path (as expected)

#JUST FOR TESTING
print('Script Parameter FullPath_Source: ', FullPath_Source)
#print('Script Parameter ConfigurationColumn: ', ConfigurationColumn)
#print('Script Parameter ConfigurationFile: ', ConfigurationFile)
#print('Script Parameter FullPath_Root: ', FullPath_Root)

#JUST FOR TESTING
print('Local Variable AllInboundFolders: ', AllInboundFolders)
#print('DelimiterDefault: ', DelimiterDefault)
#print('Local Variable FullPath_Configurations: ', Configuration)
#print('Local Variable FullPath_Configurations_File: ', FullPath_Configurations_File)
#print('Local Variable FullPath_Configurations_Column: ', FullPath_Configurations_Column)

#****************************************************************************************
#ENTRY
#****************************************************************************************
if __name__ == '__main__':
    Main()
