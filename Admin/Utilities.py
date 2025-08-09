import csv
import os
import pandas as pd
import uuid
from datetime import datetime
from pathlib import Path

#Empty/static variables
CallingObject = ''
ColumnConfigurationFilename = ''
Configurations_Column_All = pd.DataFrame()
Configurations_File_All = pd.DataFrame()
CurrentScriptFile = os.path.realpath(__file__)
DelimiterDefault = r'|'
ExpectedDelimiter = ''
FileDefinition_Configuration_Column = [
    'ColumnName_Bronze'
    , 'ColumnName_File'
    , 'ColumnName_Silver'
    , 'ColumnName_Gold'
    , 'ConfigurationColumnOrder'
    , 'ConfigurationFileID'
    , 'Datatype'
    , 'Transformation_FileToBronze'
]
FileDefinition_Configuration_File = [
    'Account'
    , 'ConfigurationFileID'
    , 'DefaultCategory'
    , 'Delimiter'
    , 'Source'
    , 'TextQualifier'
]
FileDefinition_Log = [
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
FullPath_Admin = ''
FullPath_Bronze = ''
FullPath_Bronze_Archive = ''
FullPath_Bronze_Error = ''
FullPath_Bronze_Inbound = ''
FullPath_Configurations_File = ''
FullPath_Configurations_Column = ''
FullPath_Gold = ''
FullPath_Gold_Dimensions = ''
FullPath_Gold_Error = ''
FullPath_Gold_Facts = ''
FullPath_Gold_Inbound = ''
FullPath_LogFile = ''
FullPath_Root = ''
FullPath_Silver = ''
FullPath_Silver_Dimensions = ''
FullPath_Silver_Error = ''
FullPath_Silver_Facts = ''
FullPath_Silver_Inbound = ''
IsValid_LogFile = False
Result_Success = r'Success'
Severity_Error = r'Error'
Severity_Info = r'Info'

def BuildErrorMessage(CurrentFunction, CurrentScriptFile, e, LogError, Parameters = ''):
    ErrorMessage = f'Error in {CurrentScriptFile} > {CurrentFunction}() on line {str(e.__traceback__.tb_lineno)}: {str(e)}'
    if not LogError: ErrorMessage = f'Error in {CurrentScriptFile} > {CurrentFunction}() on line {str(e.__traceback__.tb_lineno)}\r\nError: {str(e)}\r\nParameters: {Parameters}'
    return ErrorMessage

def BuildFolderPath(CallStack, Folder, SubFolder, LogEntries, ParentExecutionGUID):
    Begin = datetime.now()
    CurrentFunction = 'BuildFolderPath'
    CallStack = f'{CallStack} > {CurrentFunction}' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'Folder': Folder
        , 'ParentExecutionGUID': ParentExecutionGUID
        , 'SubFolder': SubFolder
    }
    Result = Result_Success
    try:
        if Folder != '' and not os.path.exists(Folder):
            os.makedirs(Folder, exist_ok = True)
            LogStep(Begin, CallingObject, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = f'Created {Folder}', Severity = Severity_Info)
        FullPath = os.path.join(Folder, SubFolder)
        if FullPath != '' and not os.path.exists(FullPath):
            os.makedirs(FullPath, exist_ok = True)
            LogStep(Begin, CallingObject, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = f'Created {FullPath}', Severity = Severity_Info)

    except Exception as e:
        Result = BuildErrorMessage(CurrentFunction, CurrentScriptFile, e, True) #Build the error message
        LogStep(Begin, CallingObject, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Error) #Log the error
        print(Result) #Report the error

    finally:
        #Return the result
        return Result, LogEntries, FullPath

def LogStep(Begin, Caller, CallStack, ExecutionGUID, LogEntries, Parameters, **VariedParameters): #The explicit parameters are required; anything passed into **VariedParameters is optional; different parameters may be passed into **VariedParameters
    #Don't log this function
    CurrentFunction = 'LogStep'
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
        Result = BuildErrorMessage(CurrentFunction, CurrentScriptFile, e, False, Parameters) #Build the error message
        print(Result) #Report the error

    finally:
        return LogEntries

def MoveFile(CallStack, FullPath_SourceFile, FullPath_TargetFile, LogEntries, ParentExecutionGUID):
    Begin = datetime.now()
    CurrentFunction = 'MoveFile'
    CallStack = f'{CallStack} > {CurrentFunction}' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'FullPath_SourceFile': FullPath_SourceFile
        , 'FullPath_TargetFile': FullPath_TargetFile
        , 'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Result_Success
    try:
        #Validate the filenames & folder paths
        if(FullPath_SourceFile == ''): raise Exception('FullPath_SourceFile cannot be empty')
        if(FullPath_TargetFile == ''): raise Exception('FullPath_TargetFile cannot be empty')

        #Split the full paths into folder paths and filenames
        FolderPath_Source = os.path.dirname(FullPath_SourceFile)
        FolderPath_Target = os.path.dirname(FullPath_TargetFile)
        Filename_Source = os.path.basename(FullPath_SourceFile)
        Filename_Target = os.path.basename(FullPath_TargetFile)

        #Create the folders if they don't exist
        if(FolderPath_Source != '') and not os.path.exists(FolderPath_Source): os.makedirs(FolderPath_Source, exist_ok = True)
        if(FolderPath_Target != '') and not os.path.exists(FolderPath_Target): os.makedirs(FolderPath_Target, exist_ok = True)

        #Build the full paths to the source and target files
        FullPath_SourceFile = os.path.join(FolderPath_Source, Filename_Source)
        FullPath_TargetFile = os.path.join(FolderPath_Target, Filename_Target)

        #Move the file
        os.rename(FullPath_SourceFile, FullPath_TargetFile)

        #Log the step
        LogStep(Begin, CallingObject, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)

    except Exception as e:
        Result = BuildErrorMessage(CurrentFunction, CurrentScriptFile, e, True) #Build the error message
        LogStep(Begin, CallingObject, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Error) #Log the error
        print(Result) #Report the error

    finally:
        #Return the result
        return Result, LogEntries

def RetrieveOrCreateFile(CallStack, FileDefinition, FullPath, LogEntries, ParentExecutionGUID):
    Begin = datetime.now()
    CurrentFunction = 'RetrieveOrCreateFile'
    CallStack = f'{CallStack} > {CurrentFunction}' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Result_Success
    try:
        #Validate the FullPath parameter
        if(FullPath == ''): raise Exception('FullPath cannot be empty')

        #Validate the folder path; create it if it doesn't exist
        if(FullPath != '') and not os.path.exists(FullPath): os.makedirs(FullPath, exist_ok = True)

        #Validate the file existence; create it if it's not found
        File = pd.DataFrame() #Initialize to an empty DataFrame
        if(FullPath != '') and os.path.exists(FullPath): File = pd.read_csv(FullPath, delimiter = DelimiterDefault, quoting = csv.QUOTE_NONE)
        if len(File) == 0:
            #Create an empty file
            os.makedirs(os.path.dirname(FullPath), exist_ok = True)
            with open(FullPath, 'w') as f: f.write(DelimiterDefault.join(FileDefinition) + '\n')
            with open(FullPath, 'w') as f: f.write(FileDefinition) #Add the correct column headers
            Result = 'Log file was not found and therefore created: ' + FullPath #Return the warning but continue and don't fail
            File = pd.read_csv(FullPath, delimiter = DelimiterDefault)
        else:
            #Validate the actual column headers
            ActualColumnsAsList = File.columns.tolist()
            Result, Issue, LogEntries = ValidateColumnHeader(ActualColumnsAsList, CallStack, FileDefinition, LogEntries, ParentExecutionGUID)
            if(Result != Result_Success): raise Exception('Error in ValidateColumnHeader') #Log the error and don't continue

        #Log the step
        LogStep(Begin, CallingObject, CallStack, ExecutionGUID, LogEntries, Parameters, File = FullPath, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)

    except Exception as e:
        Result = BuildErrorMessage(CurrentFunction, CurrentScriptFile, e, True) #Build the error message
        LogStep(Begin, CallingObject, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Error) #Log the error
        print(Result) #Report the error

    finally:
        #Return the result
        return Result, File, LogEntries

def RetrieveConfigurations_Column(CallStack, LogEntries, ParentExecutionGUID):
    #Variable(s) defined outside of this function, but set within this function
    global Configurations_Column_All

    #Get all column level configurations for the specified ConfigurationFileID
    Begin = datetime.now()
    CurrentFunction = 'RetrieveConfigurations_Column'
    CallStack = f'{CallStack} > {CurrentFunction}' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Result_Success
    try:
        ConfigurationFile = pd.DataFrame() #Initialize to an empty DataFrame
        Result, ConfigurationFile, LogEntries = RetrieveOrCreateFile(CallStack, FileDefinition_Configuration_Column, FullPath_Configurations_Column, LogEntries, ParentExecutionGUID)
        if(Result == Result_Success): Configurations_Column_All = ConfigurationFile

        #Log the step
        LogStep(Begin, CallingObject, CallStack, ExecutionGUID, LogEntries, Parameters, File = FullPath_Configurations_Column, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)

    except Exception as e:
        Result = BuildErrorMessage(CurrentFunction, CurrentScriptFile, e, True) #Build the error message
        LogStep(Begin, CallingObject, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Error) #Log the error
        print(Result) #Report the error

    finally:
        #Return the result
        return Result, LogEntries

def RetrieveConfigurations_File(CallStack, LogEntries, ParentExecutionGUID):
    #Variable(s) defined outside of this function, but set within this function
    global Configurations_File_All

    #Get all file level configurations for the current source
    Begin = datetime.now()
    CurrentFunction = 'RetrieveConfigurations_File'
    CallStack = f'{CallStack} > {CurrentFunction}' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'FullPath_Configurations_File': FullPath_Configurations_File
        , 'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Result_Success
    try:
        ConfigurationFile = pd.DataFrame() #Initialize to an empty DataFrame
        Result, ConfigurationFile, LogEntries = RetrieveOrCreateFile(CallStack, FileDefinition_Configuration_File, FullPath_Configurations_File, LogEntries, ParentExecutionGUID)
        if(Result == Result_Success): Configurations_File_All = ConfigurationFile

        #Log the step
        LogStep(Begin, CallingObject, CallStack, ExecutionGUID, LogEntries, Parameters, File = FullPath_Configurations_File, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)
        
    except Exception as e:
        Result = BuildErrorMessage(CurrentFunction, CurrentScriptFile, e, True) #Build the error message
        LogStep(Begin, CallingObject, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Error) #Log the error
        print(Result) #Report the error

    finally:
        #Return the result
        return Result, LogEntries

def SetGlobalVariables(Caller, CallStack, LogEntries, ParentExecutionGUID):
    #Set the global variables
    global CallingObject
    global DelimiterDefault
    global ExpectedDelimiter
    global FullPath_Admin
    global FullPath_Bronze
    global FullPath_Bronze_Archive
    global FullPath_Bronze_Error
    global FullPath_Bronze_Inbound
    global FullPath_Configurations_Column
    global FullPath_Configurations_File
    global FullPath_Gold
    global FullPath_Gold_Dimensions
    global FullPath_Gold_Error
    global FullPath_Gold_Facts
    global FullPath_Gold_Inbound
    global FullPath_LogFile
    global FullPath_Root
    global FullPath_Silver
    global FullPath_Silver_Dimensions
    global FullPath_Silver_Error
    global FullPath_Silver_Facts
    global FullPath_Silver_Inbound

    Begin = datetime.now()
    CurrentFunction = 'SetGlobalVariables'
    CallStack = f'{CallStack} > {CurrentFunction}' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Result_Success
    try:
        CallingObject = Caller
        DelimiterDefault = r'|'

        #Set & validate folder paths
        Result, LogEntries, FullPath_Root =              BuildFolderPath(CallStack, str(Path(__file__).parent.parent), '', LogEntries, ParentExecutionGUID)
        Result, LogEntries, FullPath_Admin =             BuildFolderPath(CallStack, FullPath_Root, 'Admin', LogEntries, ParentExecutionGUID)
        Result, LogEntries, FullPath_Bronze =            BuildFolderPath(CallStack, FullPath_Root, 'Bronze', LogEntries, ParentExecutionGUID)
        Result, LogEntries, FullPath_Bronze_Archive =    BuildFolderPath(CallStack, FullPath_Bronze, 'Archive', LogEntries, ParentExecutionGUID)
        Result, LogEntries, FullPath_Bronze_Error =      BuildFolderPath(CallStack, FullPath_Bronze, 'Error', LogEntries, ParentExecutionGUID)
        Result, LogEntries, FullPath_Bronze_Inbound =    BuildFolderPath(CallStack, FullPath_Bronze, 'Inbound', LogEntries, ParentExecutionGUID)
        Result, LogEntries, FullPath_Gold =              BuildFolderPath(CallStack, FullPath_Root, 'Gold', LogEntries, ParentExecutionGUID)
        Result, LogEntries, FullPath_Gold_Dimensions =   BuildFolderPath(CallStack, FullPath_Gold, 'Dimensions', LogEntries, ParentExecutionGUID)
        Result, LogEntries, FullPath_Gold_Error =        BuildFolderPath(CallStack, FullPath_Gold, 'Error', LogEntries, ParentExecutionGUID)
        Result, LogEntries, FullPath_Gold_Facts =        BuildFolderPath(CallStack, FullPath_Gold, 'Facts', LogEntries, ParentExecutionGUID)
        Result, LogEntries, FullPath_Gold_Inbound =      BuildFolderPath(CallStack, FullPath_Gold, 'Inbound', LogEntries, ParentExecutionGUID)
        Result, LogEntries, FullPath_Silver =            BuildFolderPath(CallStack, FullPath_Root, 'Silver', LogEntries, ParentExecutionGUID)
        Result, LogEntries, FullPath_Silver_Dimensions = BuildFolderPath(CallStack, FullPath_Silver, 'Dimensions', LogEntries, ParentExecutionGUID)
        Result, LogEntries, FullPath_Silver_Error =      BuildFolderPath(CallStack, FullPath_Silver, 'Error', LogEntries, ParentExecutionGUID)
        Result, LogEntries, FullPath_Silver_Facts =      BuildFolderPath(CallStack, FullPath_Silver, 'Facts', LogEntries, ParentExecutionGUID)
        Result, LogEntries, FullPath_Silver_Inbound =    BuildFolderPath(CallStack, FullPath_Silver, 'Inbound', LogEntries, ParentExecutionGUID)

        FullPath_Configurations_Column = os.path.join(FullPath_Admin, 'ConfigurationColumn.txt')
        FullPath_Configurations_File = os.path.join(FullPath_Admin, 'ConfigurationFile.txt')
        FullPath_LogFile = os.path.join(FullPath_Admin, 'Log.txt')

        #Log the step
        LogStep(Begin, CallingObject, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)

    except Exception as e:
        Result = BuildErrorMessage(CurrentFunction, CurrentScriptFile, e, True) #Build the error message
        LogStep(Begin, CallingObject, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Error) #Log the error
        print(Result) #Report the error

    finally:
        #Return the result
        return Result, LogEntries

def ValidateColumnHeader(ActualColumnsAsList, CallStack, ExpectedColumnsAsList, LogEntries, ParentExecutionGUID):
    Begin = datetime.now()
    CurrentFunction = 'ValidateColumnHeader'
    CallStack = f'{CallStack} > {CurrentFunction}' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Issue = ''
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
            Issue = 'ExtraColumns'
            Result = 'Extra column(s) found: ' + str(ExtraColumns)
            LogStep(Begin, CallingObject, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Error) #Log the error & continue
        if(len(MissingColumns) > 0):
            if Issue != '': Issue = Issue + '.'
            Issue += 'MissingColumns'
            if(len(Result) > 0): Result += '; '
            Result += 'Column(s) missing: ' + str(MissingColumns)
            LogStep(Begin, CallingObject, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Error) #Log the error & continue
        if(len(ExtraColumns) + len(MissingColumns) == 0):
            #Log the step
            LogStep(Begin, CallingObject, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)
        
    except Exception as e:
        Result = BuildErrorMessage(CurrentFunction, CurrentScriptFile, e, True) #Build the error message
        LogStep(Begin, CallingObject, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Error) #Log the error
        print(Result) #Report the error

    finally:
        #Return the result
        return Result, Issue, LogEntries

def ValidateLogFile(CallStack, LogEntries, ParentExecutionGUID):
    #Variable(s) defined outside of this function, but set within this function
    global IsValid_LogFile

    Begin = datetime.now()
    CurrentFunction = 'ValidateLogFile'
    CallStack = f'{CallStack} > {CurrentFunction}' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Issue = ''
    Parameters = {
        'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Result_Success
    try:
        IsValid_LogFile = False
        LogFile = pd.DataFrame() #Initialize to an empty DataFrame
        Result, LogFile, LogEntries = RetrieveOrCreateFile(CallStack, FileDefinition_Log, FullPath_LogFile, LogEntries, ParentExecutionGUID)
        if(Result == Result_Success): IsValid_LogFile = True

        #Log the step
        LogStep(Begin, CallingObject, CallStack, ExecutionGUID, LogEntries, Parameters, File = FullPath_LogFile, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)

    except Exception as e:
        Result = BuildErrorMessage(CurrentFunction, CurrentScriptFile, e, True) #Build the error message
        LogStep(Begin, CallingObject, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Error) #Log the error
        print(Result) #Report the error

    finally:
        #Return the result
        return Result, LogEntries

def WriteToLogFile(CallStack, LogEntries, ParentExecutionGUID):
    #Variable(s) defined outside of this function, but set within this function
    global FullPath_LogFile

    #Don't log the results of this function
    Begin = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') #Format the value as YYYY-MM-DD HH:MM:SS.ms
    CurrentFunction = 'WriteToLogFile'
    CallStack = f'{CallStack} > {CurrentFunction}' #Add the current function to the call stack
    Parameters = {
        'DelimiterDefault': DelimiterDefault
        , 'FullPath_Root': FullPath_Root
        , 'ParentExecutionGUID': ParentExecutionGUID
    }
    try:
        #Set the full path to the log file to /<Root>/Admin/Log.txt
        FullPath_LogFile = os.path.join(FullPath_Root, 'Admin', 'Log.txt')
        EntriesToLog = pd.DataFrame(LogEntries)
        EntriesToLog = EntriesToLog.sort_values(by = 'Begin') #Explicitly order the entries by when they occurred, otherwise they'll be ordered by when they were logged, which means the first function called would be logged last
        EntriesToLog.to_csv(FullPath_LogFile, sep = DelimiterDefault, header = False, index = False, mode = 'a') #Write the log entries to the log file
    except Exception as e:
        Result = BuildErrorMessage(CurrentFunction, CurrentScriptFile, e, False, Parameters) #Build the error message
        print(Result) #Report the error
