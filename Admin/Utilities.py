import csv
import inspect
import os
import pandas as pd
import uuid
from datetime import datetime
from pathlib import Path

Caller = os.path.realpath(__file__)
Configurations_Column_OneFile = pd.DataFrame()
DelimiterDefault = r'|'
FullPath_LogFile = ''
IsValid_LogFile = False
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

def LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, **VariedParameters): #The explicit parameters are required; anything passed into **VariedParameters is optional; different parameters may be passed into **VariedParameters
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
        print('Error in', Caller, '> LogStep on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        return LogEntries

def MoveFile(CallStack, FullPath_SourceFile, FullPath_TargetFile, LogEntries, ParentExecutionGUID):
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
        LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)

    except Exception as e:
        #Return the error
        Result = str(e)

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

        #Report the error
        print('Error in', Caller, '> MoveFile on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result, LogEntries

def RetrieveConfigurations_Column(CallStack, ConfigurationFileID, Configurations_Column_All, FullPath_Configurations_Column, LogEntries, ParentExecutionGUID):
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
        #Retrieve all column level configurations for the specified ConfigurationFileID, but only once per process execution
        if(len(Configurations_Column_All) == 0): Configurations_Column_All = pd.read_csv(FullPath_Configurations_Column, delimiter = DelimiterDefault)

        #Filter for the value in the column [ConfigurationFileID] - this will not work properly if there are multiple records in the configuration file for the specified ConfigurationFileID
        Configurations_Column_OneFile = Configurations_Column_All[Configurations_Column_All['ConfigurationFileID'] == int(ConfigurationFileID)]

        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)

    except Exception as e:
        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

        #Report the error
        print('Error in', Caller, '> RetrieveConfigurations_Column on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result, Configurations_Column_All, Configurations_Column_OneFile, LogEntries

def RetrieveConfigurations_File(CallStack, Configurations_File_All, FullPath_Configurations_File, LogEntries, ParentExecutionGUID, Source):
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
        #Retrieve all file level configurations for the specified source, but only once per process execution
        if(len(Configurations_File_All) == 0): Configurations_File_All = pd.read_csv(FullPath_Configurations_File, delimiter = DelimiterDefault)

        #Filter for the values in the column [Source]
        Configurations_File_OneFile = Configurations_File_All[Configurations_File_All['Source'] == Source]
        
        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info) #Begin, CallStack, ExecutionGUID, LogEntries, Parameters, **VariedParameters
        
    except Exception as e:
        #Return the error
        Result = str(e)

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

        #Report the error
        print('Error in', Caller, '> RetrieveConfigurations_File on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result, Configurations_File_All, Configurations_File_OneFile, LogEntries

def ValidateColumnHeader(ActualColumnsAsList, CallStack, ExpectedColumnsAsList, LogEntries, ParentExecutionGUID):
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
            LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Error) #Log the error & continue
        if(len(MissingColumns) > 0):
            Result = 'Column(s) missing: ' + str(MissingColumns)
            LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Error) #Log the error & continue
        if(len(ExtraColumns) + len(MissingColumns) == 0):
            #Log the step
            LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)
        
    except Exception as e:
        #Return the error
        Result = str(e)

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

        #Report the error
        print('Error in ValidateColumnHeader on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result, LogEntries

def ValidateLogFile(CallStack, FullPath_Root, LogEntries, ParentExecutionGUID):
    #Variable(s) defined outside of this function, but set within this function
    global FullPath_LogFile
    global IsValid_LogFile

    Begin = datetime.now()
    CallStack = CallStack + ' > ValidateLogFile' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Result_Success
    try:
        FullPath_LogFile = os.path.join(FullPath_Root, 'Admin', 'Log.txt')

        #Validate the log file existence; create it if it's not found
        if(FullPath_LogFile != '') and not os.path.exists(FullPath_LogFile):
            #Create an empty file
            os.makedirs(os.path.dirname(FullPath_LogFile), exist_ok = True)
            with open(FullPath_LogFile, 'w') as f: f.write('|'.join(LogFileDefinition) + '\n')
            with open(FullPath_LogFile, 'w') as f: f.write(LogFileDefinition) #Add the correct column headers
            Result = 'Log file was not found and therefore created: ' + FullPath_LogFile #Return the warning but continue and don't fail

        #Put actual column headers into a list
        LogFile = pd.read_csv(FullPath_LogFile, delimiter = DelimiterDefault)
        ActualColumnsAsList = LogFile.columns.tolist()

        #Validate the actual column headers
        Result, LogEntries = ValidateColumnHeader(ActualColumnsAsList, CallStack, LogFileDefinition, LogEntries, ParentExecutionGUID)
        IsValid_LogFile = (Result == Result_Success)

        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, File = FullPath_LogFile, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)

    except Exception as e:
        #Return the error
        Result = str(e)

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

        #Report the error
        print('Error in', Caller, '> ValidateLogFile on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result, FullPath_LogFile, IsValid_LogFile, LogEntries

def WriteToLogFile(DelimiterDefault, FullPath_LogFile, LogEntries):
    #Don't log the results of this function
    try:
        EntriesToLog = pd.DataFrame(LogEntries)
        EntriesToLog = EntriesToLog.sort_values(by = 'Begin') #Explicitly order the entries by when they occurred, otherwise they'll be ordered by when they were logged, which means the first function called would be logged last
        EntriesToLog.to_csv(FullPath_LogFile, sep = DelimiterDefault, header = False, index = False, mode = 'a')
    except Exception as e:
        #Report the error
        print('Error in', Caller, '> WriteToLogFile on line', e.__traceback__.tb_lineno, ':', str(e))
