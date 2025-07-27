import os
import pandas as pd
import uuid
from datetime import datetime
from pathlib import Path

#Empty/static variables
Caller = os.path.realpath(__file__)
ColumnConfigurationFilename = ''
Configurations_Column_All = pd.DataFrame()
Configurations_File_All = pd.DataFrame()
DelimiterDefault = r'|'
ExpectedDelimiter = ''
FullPath_Archive = ''
FullPath_Bronze = ''
FullPath_Bronze_Transaction = ''
FullPath_Configurations = ''
FullPath_Configurations_File = ''
FullPath_Configurations_Column = ''
FullPath_Error = ''
FullPath_Inbound = ''
FullPath_LogFile = ''
FullPath_Root = ''
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

def RetrieveConfigurations_Column(CallStack, LogEntries, ParentExecutionGUID):
    #Variable(s) defined outside of this function, but set within this function
    global Configurations_Column_All

    #Get all column level configurations for the specified ConfigurationFileID
    Begin = datetime.now()
    CallStack = CallStack + ' > RetrieveConfigurations_Column' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Result_Success
    try:
        #Retrieve all column level configurations for the specified ConfigurationFileID, but only once per process execution
        if(len(Configurations_Column_All) == 0): Configurations_Column_All = pd.read_csv(FullPath_Configurations_Column, delimiter = DelimiterDefault)

        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, File = FullPath_Configurations_Column, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)

    except Exception as e:
        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, File = FullPath_Configurations_Column, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

        #Report the error
        print('Error in', Caller, '> RetrieveConfigurations_Column on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result, LogEntries

def RetrieveConfigurations_File(CallStack, LogEntries, ParentExecutionGUID):
    #Variable(s) defined outside of this function, but set within this function
    global Configurations_File_All

    #Get all file level configurations for the current source
    Begin = datetime.now()
    CallStack = CallStack + ' > RetrieveConfigurations_File' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'FullPath_Configurations_File': FullPath_Configurations_File
        , 'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Result_Success
    try:
        #Retrieve all file level configurations for the specified source, but only once per process execution
        Configurations_File_All = pd.read_csv(FullPath_Configurations_File, delimiter = DelimiterDefault)

        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, File = FullPath_Configurations_File, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info) #Begin, CallStack, ExecutionGUID, LogEntries, Parameters, **VariedParameters
        
    except Exception as e:
        #Return the error
        Result = str(e)

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, File = FullPath_Configurations_File, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

        #Report the error
        print('Error in', Caller, '> RetrieveConfigurations_File on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result, LogEntries

def SetGlobalVariables(CallStack, LogEntries, ParentExecutionGUID):
    #Set the global variables
    global DelimiterDefault
    global ExpectedDelimiter
    global FullPath_Archive
    global FullPath_Bronze
    global FullPath_Bronze_Transaction
    global FullPath_Configurations
    global FullPath_Configurations_Column
    global FullPath_Configurations_File
    global FullPath_Error
    global FullPath_Inbound
    global FullPath_LogFile
    global FullPath_Root

    Begin = datetime.now()
    CallStack = CallStack + ' > SetGlobalVariables' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Result_Success
    try:
        DelimiterDefault = r'|'
        FullPath_Root = str(Path(__file__).parent.parent) #Get the root path of the project; set first so that all other paths can be set relative to it
        FullPath_Archive = os.path.join(FullPath_Root, 'Archive')
        FullPath_Bronze = os.path.join(FullPath_Root, 'Bronze')
        FullPath_Bronze_Transaction = os.path.join(FullPath_Bronze, 'Bronze.Transaction.txt')
        FullPath_Configurations = os.path.join(FullPath_Root, 'Admin')
        FullPath_Configurations_Column = os.path.join(FullPath_Configurations, 'ConfigurationColumn.txt')
        FullPath_Configurations_File = os.path.join(FullPath_Configurations, 'ConfigurationFile.txt')
        FullPath_Error = os.path.join(FullPath_Root, 'Error')
        FullPath_Inbound = os.path.join(FullPath_Root, 'Inbound')

        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Info)

    except Exception as e:
        #Return the error
        Result = str(e)

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = str(e), Severity = Severity_Error)

        #Report the error
        print('Error in', Caller, '> SetGlobalVariables on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result, LogEntries

def ValidateColumnHeader(ActualColumnsAsList, CallStack, ExpectedColumnsAsList, LogEntries, ParentExecutionGUID):
    Begin = datetime.now()
    CallStack = CallStack + ' > ValidateColumnHeader' #Add the current function to the call stack
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
            Issue = str(ExtraColumns)
            Result = 'Extra column(s) found: ' + str(ExtraColumns)
            LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Severity_Error) #Log the error & continue
        if(len(MissingColumns) > 0):
            if Issue != '': Issue = Issue + '.'
            Issue += 'MissingColumns'
            if(len(Result) > 0): Result += '; '
            Result += 'Column(s) missing: ' + str(MissingColumns)
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
        return Result, Issue, LogEntries

def ValidateLogFile(CallStack, LogEntries, ParentExecutionGUID):
    #Variable(s) defined outside of this function, but set within this function
    global FullPath_LogFile
    global IsValid_LogFile

    Begin = datetime.now()
    CallStack = CallStack + ' > ValidateLogFile' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Issue = ''
    Parameters = {
        'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Result_Success
    try:
        #Validate the log file existence; create it if it's not found
        FullPath_LogFile = os.path.join(FullPath_Root, 'Admin', 'Log.txt')
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
        Result, Issue, LogEntries = ValidateColumnHeader(ActualColumnsAsList, CallStack, LogFileDefinition, LogEntries, ParentExecutionGUID)
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
        return Result, IsValid_LogFile, LogEntries

def WriteToLogFile(CallStack, LogEntries, ParentExecutionGUID):
    #Variable(s) defined outside of this function, but set within this function
    global FullPath_LogFile

    #Don't log the results of this function
    Begin = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') #Format the value as YYYY-MM-DD HH:MM:SS.ms
    CallStack = CallStack + ' > WriteToLogFile' #Add the current function to the call stack
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
        #Report the error
        print('Error at', Begin, 'in', Caller, '> WriteToLogFile on line', e.__traceback__.tb_lineno, ':', str(e), '\n', 'Parameters:', Parameters)
