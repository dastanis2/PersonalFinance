#****************************************************************************************
""""
DESCRIPTION
    Load all valid files into the Bronze layer 

STEPS
	1. Set the value of script-wide variables
	2. Validate the log file
	3. Validate root parameters
   	4. Retrieve all file level configuration values
	5. Retrieve all column level configuration values
    6. Confirm Bronze Transaction layout
	7. For each sub-folder in the Inbound folder
    	7.1. Filter file level configuration values for the current file
		7.2. Filter column level configuration values for the current file
		7.3. For each file in the current Inbound sub-folder
			7.3.1. Validate the column header of the current file
			7.3.2. Copy the current file to the in-memory Bronze table
			7.3.3. Archive the current file
        7.4. Copy the in-memory Bronze table to the Bronze Transaction file
	8. Write all steps and errors to the log file
"""
#****************************************************************************************

#****************************************************************************************
#REFERENCES
#****************************************************************************************
import datetime
import os
import pandas as pd
import Utilities
import uuid
from datetime import datetime
from pathlib import Path

#****************************************************************************************
#PARAMETERS
#   These need to be set by whatever calls this script
#****************************************************************************************
InboundSourceFolder = r''
ParentExecutionGUID = '' #This is set by the calling object (script or otherwise); it is used to link all steps and errors together for the entire process in the log file

#****************************************************************************************
#GLOBAL VARIABLES
#   Set these variables to either empty or hard-coded values
#   These variables can change value by any function
#****************************************************************************************
AllInboundFolders = True
Bronze = pd.DataFrame()
Bronze_Transaction_Expected = [
    'Amount'
    , 'Category'
    , 'Date'
    , 'Description'
    , 'ExecutionGUID'
    , 'IngestDatetime'
    , 'Source'
    , 'SourceFile'
]
Bronze_Transaction_Existing = pd.DataFrame()
Bronze_Transaction_New = pd.DataFrame(columns = Bronze_Transaction_Expected)
Caller = os.path.realpath(__file__)
ConfigurationFileID = 0
Configurations_Column_CurrentFile = pd.DataFrame()
Configurations_File_CurrentFile = pd.DataFrame()
ExpectedDelimiter = ''
InboundFileFound = False
IsValid_LogFile = False
LogEntries = []

#****************************************************************************************
#FUNCTIONS
#****************************************************************************************
def CopyToBronze(CallStack, ParentExecutionGUID, ValidRecords):
    #Variable(s) defined outside of this function, but set within this function
    global Bronze
    global Bronze_Transaction_New

    Begin = datetime.now()
    CallStack = CallStack + ' > CopyToBronze' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Utilities.Result_Success
    try:

        #Remove duplicates
        RecordsToCopy = ValidRecords.drop_duplicates(keep = 'last')

        #Get the row count of the records to copy to bronze
        RowCount = len(RecordsToCopy)

        #If there are records to copy
        if(RowCount > 0):
            #Apply any formulas to the columns in RecordsToCopy (such as data type conversions, column replacments, etc.)
            for _, config_row in Configurations_Column_CurrentFile.drop_duplicates(subset = ['ColumnName_Bronze']).iterrows():
                bronze_col = config_row['ColumnName_Bronze']
                formula = config_row.get('Transformation_FileToBronze', None)
                if bronze_col and isinstance(formula, str) and formula.strip(): #Only create and populate if there is a formula
                    RecordsToCopy[bronze_col] = RecordsToCopy.apply(
                        lambda row: eval(formula, {'row': row, 'pd': pd, 'IngestDatetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}) #Put the formula for IngestDatetime here so that it is written only once (instead of multiple times in the column level configuration file) but applied to every row, regardless of file or source
                        , axis = 1
                    )

            #Map columns from File to Bronze without creating duplicates
            seen = set()
            unique_mapping = {}
            for file_col, bronze_col in zip(Configurations_Column_CurrentFile['ColumnName_File'], Configurations_Column_CurrentFile['ColumnName_Bronze']):
                if pd.notnull(file_col) and pd.notnull(bronze_col) and bronze_col not in seen:
                    unique_mapping[file_col] = bronze_col
                    seen.add(bronze_col)
            ColumnMapping = unique_mapping
            RecordsToCopy = RecordsToCopy.rename(columns = ColumnMapping)

            #Align RecordsToCopy to ExistingBronze columns by dropping any columns not in ExistingBronze
            RecordsToCopy = RecordsToCopy[[col for col in Bronze_Transaction.columns if col in RecordsToCopy.columns]]

            #Copy the records to the Bronze Transaction file
            RecordsToCopy.to_csv(Utilities.FullPath_Bronze_Transaction, sep = Utilities.DelimiterDefault, header = False, index = False, mode = 'a')

        #Clear out Bronze so that only it will contain only records from the current source
        Bronze = pd.DataFrame()

        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, File = Utilities.FullPath_Bronze_Transaction, ParentExecutionGUID = ParentExecutionGUID, Result = Result, RowCount = RowCount, Severity = Utilities.Severity_Info)

    except Exception as e:
        ErrorMessage = 'Error on line ' + str(e.__traceback__.tb_lineno) + ': ' + str(e)

        #Return the error
        Result = ErrorMessage

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, File = Utilities.FullPath_Bronze_Transaction, ParentExecutionGUID = ParentExecutionGUID, Result = ErrorMessage, Severity = Utilities.Severity_Error)

        #Report the error
        print('Error in', Caller, '> CopyToBronze on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result

def LogStep(Begin, CallStack, ExecutionGUID, Parameters, **VariedParameters): #The explicit parameters are required; anything passed into **VariedParameters is optional; different parameters may be passed into **VariedParameters
    #Even though this local function calls another of the same name in a different script, keep this local function to be able to use "**VariedParameters"
    #Variable(s) defined outside of this function, but set within this function
    global LogEntries
    #Add the step to the current set
    LogEntries = Utilities.LogStep(Begin, CallStack, ExecutionGUID, LogEntries, Parameters, **VariedParameters)

def Main():
    #Variable(s) defined outside of this function, but set within this function
    global Bronze_Transaction
    global LogEntries

    #Set script-wide scalar variables
    Begin = datetime.now()
    CallStack = r'Main'
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    File = ''
    Issue = ''
    IsValid_BronzeTransactionFile = False
    IsValid_LogFile = False
    Parameters = {
        'InboundSourceFolder': InboundSourceFolder
    }
    Result = Utilities.Result_Success

    try:
        #Set global variables
        Result, LogEntries = Utilities.SetGlobalVariables(CallStack, LogEntries, ExecutionGUID)
        if(Result != Utilities.Result_Success): raise Exception('Error in SetGlobalVariables') #Log the error and don't continue

        #Set script-wide calculated variables - included in the "try/except" block in case an expression fails
        if(InboundSourceFolder == ''): AllInboundFolders = True #If no source folder was specified
        elif(Utilities.FullPath_Root not in InboundSourceFolder): AllInboundFolders = False #If source folder was specified, but not the full path (as expected)

        #Validate root level parameters
        Result = ValidateRootParameters(CallStack, ExecutionGUID)
        if(Result != Utilities.Result_Success): raise Exception('Error in ValidateRootParameters') #Log the error and don't continue

        #Validate the log file so that all subsequent steps & errors can be properly logged
        IsValid_LogFile = False #The log file is invalid until proven otherwise
        Result, IsValid_LogFile, LogEntries = Utilities.ValidateLogFile(CallStack, LogEntries, ExecutionGUID)
        if(Result != Utilities.Result_Success): raise Exception(Result) #Report the error and don't continue - can't log the error because log file is invalid
        IsValid_LogFile = True #Mark the log file as valid only after it's passed all validation; otherwise, consider it invalid

        #Get all file level configurations; do this only once per process execution
        Result, LogEntries = Utilities.RetrieveConfigurations_File(CallStack, LogEntries, ExecutionGUID)
        if(Result != Utilities.Result_Success): raise Exception('Error in RetrieveConfigurations_File') #Log the error and don't continue

        #Get all column level configurations for the current ConfigurationFileID; do this only once per process execution
        Result, LogEntries = Utilities.RetrieveConfigurations_Column(CallStack, LogEntries, ExecutionGUID)
        if(Result != Utilities.Result_Success): raise Exception('Error in RetrieveConfigurations_Column') #Log the error and don't continue

        #Confirm Bronze Transaction layout (should never change, but confirm it anyway)
        Bronze_Transaction = pd.read_csv(Utilities.FullPath_Bronze_Transaction, delimiter = Utilities.DelimiterDefault)
        ActualColumnsAsList = Bronze_Transaction.columns.tolist()
        Result, Issue, LogEntries = Utilities.ValidateColumnHeader(ActualColumnsAsList, CallStack, Bronze_Transaction_Expected, LogEntries, ExecutionGUID)
        if(Result != Utilities.Result_Success): 
            print('Error in ValidateColumnHeader:', Result)
            raise Exception('Error in ValidateColumnHeader') #Log the error and don't continue

        #Mark the Bronze Transaction file as valid only after it's passed all validation; otherwise, consider it invalid
        IsValid_BronzeTransactionFile = True

        Result = Utilities.Result_Success

    except Exception as e:
        #Report the error
        print('Error in', Caller, '> Main on line', e.__traceback__.tb_lineno, ':', str(e))

    try:
        if(IsValid_LogFile & IsValid_BronzeTransactionFile):
            #Loop through the Inbound folder & sub-folder(s) looking for files to ingest
            RootInboundFolder = os.path.join(Utilities.FullPath_Root, 'Inbound')
            if AllInboundFolders: #Process files in all Inbound sub-folders
                for FolderName in os.listdir(RootInboundFolder):
                    #Loop through all files in the current Inbound sub-folder
                    InboundFolder = os.path.join(RootInboundFolder, FolderName)
                    Result = ProcessInboundFolder(CallStack, InboundFolder, ExecutionGUID)
            else: #Process files in just the specified Inbound sub-folder
                #Loop through all files in the specified Inbound sub-folder
                InboundFolder = os.path.join(RootInboundFolder, InboundSourceFolder)
                Result = ProcessInboundFolder(CallStack, InboundFolder, ExecutionGUID)

        if not InboundFileFound: Result = Utilities.Result_Success

        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Utilities.Severity_Info)

    except Exception as e:
        #Return the error
        Result = str(e)

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, File = File, Result = str(e), Severity = Utilities.Severity_Error)

        #Report the error
        print('Error in', Caller, '> Main on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        if IsValid_LogFile: Utilities.WriteToLogFile(CallStack, LogEntries, ExecutionGUID)

def ProcessInboundFile(CallStack, FileName, ParentExecutionGUID, Source):
    #Variable(s) defined outside of this function, but set within this function
    global Bronze
    global LogEntries

    Begin = datetime.now()
    InboundFile = ''
    CallStack = CallStack + ' > ProcessInboundFile' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'FileName': FileName
        , 'ParentExecutionGUID': ParentExecutionGUID
        , 'Source': Source
    }
    RowCount = 0
    Result = Utilities.Result_Success
    try:
        FileExtension = os.path.splitext(FileName)[1] #Get the file extension of the current file
        #Process only .csv or .txt files
        if FileName.lower().endswith(('.csv', '.txt')):
            InboundFile = os.path.join(Utilities.FullPath_Inbound, Source, FileName) #Generate the full path and file name of the file

            #Read the current file into a dataframe
            CurrentFile = pd.read_csv(InboundFile, delimiter = ExpectedDelimiter)

            #Add "standard" columns
            CurrentFile['ExecutionGUID'] = ExecutionGUID #Add the ExecutionGUID
            CurrentFile['IngestDatetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') #Add the current date/time to the IngestDatetime column
            CurrentFile['Source'] = Source #Add the Inbound source folder
            CurrentFile['SourceFile'] = FileName #Add the file name of the current file

            if not(CurrentFile.empty): #Continue only if there is data in the current file
                #Put actual column headers into a list
                ActualColumnsAsList = CurrentFile.columns.tolist()

                #Put expected column headers into a list
                ExpectedColumnsAsList = Configurations_Column_CurrentFile['ColumnName_File'].values.tolist() #Use the values in the column [ColumnName_File] in the column level configurations
                ExpectedColumnsAsList = [x for x in ExpectedColumnsAsList if pd.notnull(x)] #Remove any null values from the list

                #Validate the actual column header
                Issue = ''
                Result, Issue, LogEntries = Utilities.ValidateColumnHeader(ActualColumnsAsList, CallStack, ExpectedColumnsAsList, LogEntries, ParentExecutionGUID)
                if(Result != Utilities.Result_Success): #There was an issue validating the column header
                    #Rename the file to indicate the issue found
                    FileName = FileName.replace(FileExtension, '') + '.InvalidColumnHeader.' + Issue + FileExtension #Change the file name to indicate that it has an invalid column header
                    FullPath_Error_CurrentSource = os.path.join(Utilities.FullPath_Error, Source) #Set the path of the error folder, including the current source folder
                    if(FullPath_Error_CurrentSource != '') and not os.path.exists(FullPath_Error_CurrentSource): os.makedirs(FullPath_Error_CurrentSource, exist_ok = True) #Create the error folder if it doesn't exist
                    FullPath_Error_CurrentFile = os.path.join(FullPath_Error_CurrentSource, FileName) #Set the full path of the error file
                    
                    #Move the file to the appropriate Error folder                    
                    Result, LogEntries = Utilities.MoveFile(CallStack, InboundFile, FullPath_Error_CurrentFile, LogEntries, ParentExecutionGUID)
                else: #The file passed validation
                    #Load the file to the in-memory Bronze table, which should only contain records from all processed files in the current Inbound sub-folder
                    RowCount = len(CurrentFile) #Get the row count of the current file
                    if(len(Bronze) == 0): Bronze = CurrentFile #If the Bronze dataframe is empty, set it to the current file
                    else: Bronze.concat([Bronze, CurrentFile], axis = 1) #Otherwise, append the current file to it

                    #Archive the file
                    FullPath_Archive_CurrentSource = os.path.join(Utilities.FullPath_Archive, Source) #Set the path of the archive folder, including the current source folder
                    if(FullPath_Archive_CurrentSource != '') and not os.path.exists(FullPath_Archive_CurrentSource): os.makedirs(FullPath_Archive_CurrentSource, exist_ok = True) #Create the archive folder if it doesn't exist
                    FullPath_TargetFile = os.path.join(FullPath_Archive_CurrentSource, FileName)
                    Result, LogEntries = Utilities.MoveFile(CallStack, InboundFile, FullPath_TargetFile, LogEntries, ParentExecutionGUID)
                    if(Result != Utilities.Result_Success): raise Exception('Error in MoveFile') #Log the error and don't continue

        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, File = InboundFile, ParentExecutionGUID = ParentExecutionGUID, Result = Result, RowCount = RowCount, Severity = Utilities.Severity_Info)

    except Exception as e:
        ErrorMessage = 'Error on line ' + str(e.__traceback__.tb_lineno) + ': ' + str(e)

        #Return the error
        Result = ErrorMessage

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, File = InboundFile, ParentExecutionGUID = ParentExecutionGUID, Result = ErrorMessage, RowCount = RowCount, Severity = Utilities.Severity_Error)

        #Report the error
        print('Error in', Caller, '> ProcessInboundFile on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result

def ProcessInboundFolder(CallStack, InboundFolder, ParentExecutionGUID):
    #Variable(s) defined outside of this function, but set within this function
    global Bronze
    global ConfigurationFileID
    global Configurations_Column_CurrentFile
    global Configurations_File_CurrentFile
    global ExpectedDelimiter
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
    Result = Utilities.Result_Success
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

            #Parse the folder path of the current file to determine the "source" folder of the current file to help filter for the correct configurations
            Source = os.path.relpath(InboundFolder, Utilities.FullPath_Inbound) #Get only the last folder name from the path

            #Filter file level configurations by Source
            Configurations_File_CurrentFile = Utilities.Configurations_File_All[Utilities.Configurations_File_All['Source'] == Source]

            #Get ConfigurationID from the file level configurations for the current source
            ConfigurationFileID = Configurations_File_CurrentFile[['ConfigurationFileID']].iloc[0,0] #Use .iloc[0,0] to pinpoint the exact cell and exclude the column header in the return value

            #Get Delimiter of current file from file level configurations
            ExpectedDelimiter = Configurations_File_CurrentFile[['Delimiter']].iloc[0,0]

            #Filter column level configurations by ConfigurationFileID
            Configurations_Column_CurrentFile = Utilities.Configurations_Column_All[Utilities.Configurations_Column_All['ConfigurationFileID'] == int(ConfigurationFileID)]

            #Ingest each file in the current Inbound folder
            for FileName in os.listdir(InboundFolder):
                Result = ProcessInboundFile(CallStack, FileName, ExecutionGUID, Source)

            #Copy all records ingested from the current Inbound folder into the Bronze layer, even if a file failed
            Result = CopyToBronze(CallStack, ParentExecutionGUID, Bronze)
            if(Result != Utilities.Result_Success): raise Exception('Error in CopyToBronze') #Log the error and don't continue

        #Write Bronze Transaction records to file
        Result = WriteToBronzeFile(CallStack, ExecutionGUID)

        #Log the step
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = Result, Severity = Utilities.Severity_Info)

    except Exception as e:
        ErrorMessage = 'Error on line ' + str(e.__traceback__.tb_lineno) + ': ' + str(e)

        #Return the error
        Result = ErrorMessage

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = ErrorMessage, Severity = Utilities.Severity_Error)

        #Report the error
        print('Error in', Caller, '> ProcessInboundFolder on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result

def ValidateRootParameters(CallStack, ParentExecutionGUID):
    #This function is in this script instead of the Utilities script because the root paramters that it checks are specific to this script and not applicable to all scripts
    Begin = datetime.now()
    CallStack = CallStack + ' > ValidateRootParameters' #Add the current function to the call stack
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
        ErrorMessage = 'Error on line ' + str(e.__traceback__.tb_lineno) + ': ' + str(e)

        #Return the error
        Result = ErrorMessage

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = ErrorMessage, Severity = Utilities.Severity_Error)

        #Report the error
        print('Error in', Caller, '> ValidateRootParameters on line', e.__traceback__.tb_lineno, ':', str(e))

    finally:
        #Return the result
        return Result

def WriteToBronzeFile(CallStack, ParentExecutionGUID):
    Begin = datetime.now()
    CallStack = CallStack + ' > WriteToBronzeFile' #Add the current function to the call stack
    ExecutionGUID = str(uuid.uuid4()) #Generate a new GUID for logging the function
    Parameters = {
        'ParentExecutionGUID': ParentExecutionGUID
    }
    Result = Utilities.Result_Success
    try:
        #Write new Bronze Transaction records to the Bronze Transaction file
        if(len(Bronze_Transaction_New) > 0): Bronze_Transaction_New.to_csv(Utilities.FullPath_Bronze_Transaction, sep = Utilities.DelimiterDefault, header = False, index = False, mode = 'a')
    except Exception as e:
        ErrorMessage = 'Error on line ' + str(e.__traceback__.tb_lineno) + ': ' + str(e)

        #Return the error
        Result = ErrorMessage

        #Log the error
        LogStep(Begin, CallStack, ExecutionGUID, Parameters, ParentExecutionGUID = ParentExecutionGUID, Result = ErrorMessage, Severity = Utilities.Severity_Error)

        #Report the error
        print('Error in', Caller, '> WriteToBronzeFile on line', e.__traceback__.tb_lineno, ':', str(e))

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
