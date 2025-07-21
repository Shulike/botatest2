from common.db import db_conn

def ensure_schema():
    ddl = """
    ------------------------------------------------------------------
    -- PendingQuizzes
    ------------------------------------------------------------------
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='PendingQuizzes')
    BEGIN
        CREATE TABLE dbo.PendingQuizzes (
            Id              INT IDENTITY(1,1) PRIMARY KEY,
            ProcessedFileId INT            NOT NULL,
            Question        NVARCHAR(MAX)  NOT NULL,
            Options         NVARCHAR(MAX)  NOT NULL,
            Answer          NVARCHAR(200)  NOT NULL,
            Sent            BIT NOT NULL DEFAULT 0,
            Approved        BIT NULL,
            Prompted        BIT NOT NULL DEFAULT 0
        );
    END;
    ------------------------------------------------------------------
    -- QuizResults  (добавили StudentId + индекс)
    ------------------------------------------------------------------
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='QuizResults')
    BEGIN
        CREATE TABLE dbo.QuizResults (
            Id            INT IDENTITY(1,1) PRIMARY KEY,
            PendingQuizId INT           NOT NULL,
            StudentId     BIGINT        NULL,
            ChosenOption  NVARCHAR(200) NOT NULL,
            IsCorrect     BIT           NOT NULL,
            AnsweredAt    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
        );
        CREATE INDEX IX_QuizResults_Student ON dbo.QuizResults(StudentId);
    END;
    -- если таблица уже была, но столбца StudentId нет — добавим
    IF COL_LENGTH('dbo.QuizResults','StudentId') IS NULL
    BEGIN
        ALTER TABLE dbo.QuizResults ADD StudentId BIGINT NULL;
        CREATE INDEX IX_QuizResults_Student ON dbo.QuizResults(StudentId);
    END;
    ------------------------------------------------------------------
    -- Students
    ------------------------------------------------------------------
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='Students')
    BEGIN
        CREATE TABLE dbo.Students (
            Id         INT IDENTITY(1,1) PRIMARY KEY,
            TelegramId BIGINT        NOT NULL,
            DisplayName NVARCHAR(100) NULL,
            Active     BIT NOT NULL DEFAULT 1,
            AddedAt    DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END;
    ------------------------------------------------------------------
    -- QuizDeliveries
    ------------------------------------------------------------------
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='QuizDeliveries')
    BEGIN
        CREATE TABLE dbo.QuizDeliveries (
            Id           INT IDENTITY(1,1) PRIMARY KEY,
            PendingQuizId INT      NOT NULL,
            StudentId     BIGINT   NOT NULL,
            PollId        NVARCHAR(128) NULL,
            Announced     BIT NOT NULL DEFAULT 0,
            Started       BIT NOT NULL DEFAULT 0,
            SentAt        DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
        CREATE INDEX IX_QuizDeliveries_PollId ON dbo.QuizDeliveries (PollId);
    END;
    ------------------------------------------------------------------
    -- QuizSessions
    ------------------------------------------------------------------
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='QuizSessions')
    BEGIN
        CREATE TABLE dbo.QuizSessions (
            Id             INT IDENTITY(1,1) PRIMARY KEY,
            ProcessedFileId INT      NOT NULL,
            StudentId       BIGINT   NOT NULL,
            Total           INT      NOT NULL,
            Correct         INT      NOT NULL DEFAULT 0,
            StartedAt       DATETIME2 NULL,
            FinishedAt      DATETIME2 NULL,
            TimedOut        BIT NOT NULL DEFAULT 0
        );
        CREATE UNIQUE INDEX UX_Sessions
            ON dbo.QuizSessions(ProcessedFileId, StudentId);
    END;
    """
    with db_conn() as c, c.cursor() as cur:
        cur.execute(ddl)
        c.commit()

