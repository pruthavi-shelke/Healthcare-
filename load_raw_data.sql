CREATE OR ALTER PROCEDURE Proc_Sp_insert
@schema varchar(100),
@dbo varchar(100),
@tablename varchar(100),
@file varchar(1000)

AS
BEGIN

BEGIN TRY

DECLARE @trunc VARCHAR(200);
DECLARE @bulkInsert VARCHAR(200);

-- truncate table [dev_HealthConnect_raw].[dbo].[RAW_claims]
SET @trunc = 'truncate table ' +QUOTENAME(@schema)+'.'+QUOTENAME(@dbo)+'.'+QUOTENAME(@tablename);
EXECUTE (@trunc);

/* bulk Insert [dev_HealthConnect_raw].[dbo].[RAW_claims]
from 'E:\Project\Healthcare-\csv\claims.csv'
WITH(
  FIELDTERMINATOR = ',', -- column separator
  ROWTERMINATOR = '\n', 
  FIRSTROW = 2
)
*/

SET @bulkInsert = 'bulk Insert' +QUOTENAME(@schema)+'.'+QUOTENAME(@dbo)+'.'+QUOTENAME(@tablename)+ 
'from '+ ''''+@file+''''+' WITH(
  FIELDTERMINATOR = '','', 
  ROWTERMINATOR = ''\n'', 
  FIRSTROW = 2
)';
EXECUTE (@bulkInsert);

END TRY

BEGIN CATCH
	PRINT 'Error while loading data from file: ' + @file + ' to ' + @tablename;
	PRINT 'Error Message ' + ERROR_MESSAGE();
END CATCH
END;


CREATE OR ALTER PROCEDURE Proc_HealthConnect_RawLoad

AS BEGIN
BEGIN TRY

DECLARE @yesterday_date VARCHAR(100)


DECLARE @claims VARCHAR(100)
DECLARE @diagnoses VARCHAR(100)
DECLARE @encounters VARCHAR(100)
DECLARE @medications VARCHAR(100)
DECLARE @patients VARCHAR(100)
DECLARE @payers VARCHAR(100)
DECLARE @procedures VARCHAR(100)
DECLARE @providers VARCHAR(100)

-- 20251107


SET @yesterday_date = CONVERT(VARCHAR(8), DATEADD(DAY, -1, GETDATE()), 112);

SET @claims = 'E:\Project\Healthcare-\csv\claims_' + @yesterday_date + '.csv'
SET @diagnoses = 'E:\Project\Healthcare-\csv\diagnoses_' + @yesterday_date + '.csv'
SET @encounters = 'E:\Project\Healthcare-\csv\encounters_' + @yesterday_date + '.csv'
SET @medications = 'E:\Project\Healthcare-\csv\medications_' + @yesterday_date + '.csv'
SET @patients = 'E:\Project\Healthcare-\csv\patients_' + @yesterday_date + '.csv'
SET @payers = 'E:\Project\Healthcare-\csv\payers_' + @yesterday_date + '.csv'
SET @procedures = 'E:\Project\Healthcare-\csv\procedures_' + @yesterday_date + '.csv'
SET @providers = 'E:\Project\Healthcare-\csv\providers_' + @yesterday_date + '.csv'

EXECUTE Proc_Sp_insert 'dev_HealthConnect_raw','dbo','RAW_claims',@claims;
EXECUTE Proc_Sp_insert 'dev_HealthConnect_raw','dbo','RAW_diagnoses',@diagnoses;
EXECUTE Proc_Sp_insert 'dev_HealthConnect_raw','dbo','RAW_encounters',@encounters;
EXECUTE Proc_Sp_insert 'dev_HealthConnect_raw','dbo','RAW_medications',@medications;
EXECUTE Proc_Sp_insert 'dev_HealthConnect_raw','dbo','RAW_patients',@patients;
EXECUTE Proc_Sp_insert 'dev_HealthConnect_raw','dbo','RAW_payers',@payers;
EXECUTE Proc_Sp_insert 'dev_HealthConnect_raw','dbo','RAW_procedures',@procedures;
EXECUTE Proc_Sp_insert 'dev_HealthConnect_raw','dbo','RAW_providers',@providers;

END TRY

BEGIN CATCH
	PRINT 'Error Message ' + ERROR_MESSAGE();
END CATCH
END

EXECUTE Proc_HealthConnect_RawLoad;


PRINT  CONVERT(VARCHAR(8), DATEADD(DAY, -1, GETDATE()), 112);