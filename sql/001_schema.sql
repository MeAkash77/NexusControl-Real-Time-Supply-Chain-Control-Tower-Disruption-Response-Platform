IF DB_ID('NexusControl') IS NULL CREATE DATABASE NexusControl; GO
USE NexusControl; GO
CREATE TABLE dbo.Incident(
  Id BIGINT IDENTITY PRIMARY KEY, TenantId INT NOT NULL, Type VARCHAR(40) NOT NULL,
  EntityKey VARCHAR(80) NOT NULL, Severity TINYINT NOT NULL, Status VARCHAR(20) NOT NULL DEFAULT 'Open',
  AlertCount INT NOT NULL DEFAULT 1, OpenedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  SlaDueAt DATETIME2 NOT NULL, ResolvedAt DATETIME2 NULL, Remediation NVARCHAR(500) NULL);
-- Filtered index: hot path only touches open incidents
CREATE INDEX IX_Incident_Open ON dbo.Incident(TenantId, Severity DESC, SlaDueAt) INCLUDE(Type,EntityKey,AlertCount) WHERE Status <> 'Resolved';
CREATE TABLE dbo.EventLog(Id BIGINT IDENTITY PRIMARY KEY, TenantId INT, EventId UNIQUEIDENTIFIER UNIQUE, Kind VARCHAR(30), EntityKey VARCHAR(80), Payload NVARCHAR(MAX), At DATETIME2 DEFAULT SYSUTCDATETIME());
GO
-- Correlates repeated alerts on the same entity into one incident (escalates severity)
CREATE OR ALTER PROC dbo.sp_UpsertIncident @TenantId INT,@Type VARCHAR(40),@EntityKey VARCHAR(80),@Severity TINYINT,@SlaMinutes INT AS
BEGIN SET NOCOUNT ON; SET XACT_ABORT ON; BEGIN TRAN;
  UPDATE dbo.Incident WITH (UPDLOCK,HOLDLOCK) SET AlertCount+=1, Severity=IIF(@Severity>Severity,@Severity,Severity)
   WHERE TenantId=@TenantId AND Type=@Type AND EntityKey=@EntityKey AND Status<>'Resolved';
  IF @@ROWCOUNT=0 INSERT dbo.Incident(TenantId,Type,EntityKey,Severity,SlaDueAt) VALUES(@TenantId,@Type,@EntityKey,@Severity,DATEADD(MINUTE,@SlaMinutes,SYSUTCDATETIME()));
COMMIT; END
GO
CREATE OR ALTER PROC dbo.sp_GetOpenIncidents @TenantId INT AS
  SELECT Id,Type,EntityKey,Severity,Status,AlertCount,OpenedAt,SlaDueAt,CAST(IIF(SlaDueAt<SYSUTCDATETIME(),1,0) AS BIT) AS SlaBreached
  FROM dbo.Incident WHERE TenantId=@TenantId AND Status<>'Resolved' ORDER BY Severity DESC, SlaDueAt;
GO
CREATE OR ALTER PROC dbo.sp_ResolveIncident @TenantId INT,@Id BIGINT,@Remediation NVARCHAR(500) AS
  UPDATE dbo.Incident SET Status='Resolved',ResolvedAt=SYSUTCDATETIME(),Remediation=@Remediation WHERE Id=@Id AND TenantId=@TenantId;
GO
