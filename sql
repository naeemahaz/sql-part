PARTITION BY multiple columns

The PARTITION BY clause can be used to break out window averages by multiple data points (columns). You can even calculate the information you want to use to partition your data! For example, you can calculate average goals scored by season and by country, or by the calendar year (taken from the date column).

In this exercise, you will calculate the average number home and away goals scored Legia Warszawa, and their opponents, partitioned by the month in each season.




SELECT 
	date,
    season,
    home_goal,
    away_goal,
    CASE WHEN hometeam_id = 8673 THEN 'home' 
         ELSE 'away' END AS warsaw_location,
    -- Calculate average goals partitioned by season and month
    AVG(home_goal) OVER(PARTITION BY season, 
         	EXTRACT(MONTH FROM date)) AS season_mo_home,
    AVG(away_goal) OVER(PARTITION BY season, 
            EXTRACT(MONTH FROM date)) AS season_mo_away
FROM match
WHERE 
	hometeam_id = 8673 
    OR awayteam_id = 8673
ORDER BY (home_goal + away_goal) DESC;


Sliding Windows
Perform calculations relative to a current row

Rows between start and finish


Preceding

following
unbounded preceding
unbounded following 
current row

Problem

I want to delete historical data from an old partition in my SQL Server database. How can I achieve this without having downtime?
Solution

As the business grows over a period of time, it is mandatory to archive or purge unwanted historical data. The classical delete command on the partitioned table will fill up the database transaction log quickly and it may take a very long time to complete the task. In this tip, I will walkthrough a method to purge data using partition switching. This methodology is also known as “Sliding Partitioning”.
SQL Server Partition Switching

Partition switching will allow you to move a partition between source and target tables very quickly. As this is a metadata-only operation, no data movement will happen during the switching. Hence this is extremely fast. Using this methodology, the old data can be switched to a work (Staging) table and then the data in the work table can be archived and purged.

In sliding window methodology, we don’t create new files/file group when we create new partition. We purge/archive an old partition and we reuse that partition to receive new data. Hence in a repeated/circular method, we try to use the same data files/filegroups again and again.
Partition Switching Points to Consider

The following criteria must be met for partition switching:

    The target table must be empty
    Source and target tables must have identical columns and indexes
    Both the source and target table must use the same column as the partition column
    Both the source and target tables must be in the same filegroup

If you try to switch the partition without satisfying any of the criteria, the SQL Server will throw an exception and will provide a detailed error message.
SQL Server Partition Switching Syntax

Partition switching can be accomplished by using the "ALTER TABLE SWITCH" statement.
Sliding Window Partition Steps in SQL Server

There are 5 steps to implement in the Sliding Window Partition:

    Step1: Switching partition between main and work table
    Step2: Purge or archive data from the work table
    Step3: Prepare the filegroup to accept new boundaries
    Step4: Split the right most partition based on a new boundary
    Step5: Merge the old partition with the new boundary

Example Solution

In this example, I have created an Orders table with order date as the partition column.

This table will be created with 3 partitions to accept data on a monthly basis. Initially partition one will accept data until November 30, 2017 and the second partition will accept data between Dec 1, 2017 and Dec 31, 2017. The last empty partition will accept data from Jan 1, 2018 onwards.

The below script will create physical data files, file groups, partition scheme, partition function and the Order table. Also, this script will load sample data for a few months. 

USE [master]
GO

--Drop if the DB already exists 
If exists(Select name from sys.databases where name = 'Staging_TST')
Begin
   Drop database Staging_TST
End
Go

--Create the DB
CREATE DATABASE [Staging_TST]
CONTAINMENT = NONE
ON PRIMARY ( NAME = N'Staging_TST', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL11.SQL2012\MSSQL\DATA\Staging_TST.mdf' , SIZE = 28672KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB ) 
LOG ON ( NAME = N'Staging_TST_log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL11.SQL2012\MSSQL\DATA\Staging_TST_log.ldf' , SIZE = 470144KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
go

--Remove physical database files for 3 partitioning
If (Exists(Select name from sys.database_files where name='Staging_TST_01'))
Begin
   Alter Database Staging_TST
   Remove file Staging_TST_01
End
Go

If (Exists(Select name from sys.database_files where name='Staging_TST_02'))
Begin
   Alter Database Staging_TST
   Remove file Staging_TST_02
End
Go

If (Exists(Select name from sys.database_files where name='Staging_TST_03'))
Begin
   Alter Database Staging_TST
   Remove file Staging_TST_03
End
Go

----Remove file groups for 3 partitioning
If (Exists(Select name from sys.filegroups where name='Staging_TSTFG_01'))
Begin
   Alter Database Staging_TST
   Remove Filegroup Staging_TSTFG_01
End
Go

If (Exists(Select name from sys.filegroups where name='Staging_TSTFG_02'))
Begin
   Alter Database Staging_TST
   Remove Filegroup Staging_TSTFG_02
End
Go

If (Exists(Select name from sys.filegroups where name='Staging_TSTFG_03'))
Begin
   Alter Database Staging_TST
   Remove Filegroup Staging_TSTFG_03
End
Go

Use Master
go

--Create FileGroups for partitioning
ALTER DATABASE Staging_TST
ADD FILEGROUP Staging_TSTFG_01 
GO

ALTER DATABASE Staging_TST
ADD FILE 
(
NAME = [Staging_TST_01], 
FILENAME = --'K:\Program Files\Microsoft SQL Server\MSSQL11.SIR\MSSQL\Data\Staging\Staging_TST_01.ndf', 
'C:\Program Files\Microsoft SQL Server\MSSQL11.SQL2012\MSSQL\DATA\Staging\Staging_TST_01.ndf', 
SIZE = 5242880 KB, 
MAXSIZE = UNLIMITED, 
FILEGROWTH = 5242880 KB
) TO FILEGROUP Staging_TSTFG_01
GO

ALTER DATABASE Staging_TST
ADD FILEGROUP Staging_TSTFG_02 
GO

ALTER DATABASE Staging_TST
ADD FILE 
(
NAME = [Staging_TST_02], 
FILENAME = 
'C:\Program Files\Microsoft SQL Server\MSSQL11.SQL2012\MSSQL\DATA\Staging\Staging_TST_02.ndf', 
SIZE = 5242880 KB, 
MAXSIZE = UNLIMITED, 
FILEGROWTH = 5242880 KB
) TO FILEGROUP Staging_TSTFG_02
GO

ALTER DATABASE Staging_TST
ADD FILEGROUP Staging_TSTFG_03 
GO

ALTER DATABASE Staging_TST
ADD FILE 
(
NAME = [Staging_TST_03], 
FILENAME = 
'C:\Program Files\Microsoft SQL Server\MSSQL11.SQL2012\MSSQL\DATA\Staging\Staging_TST_03.ndf', 
SIZE = 5242880 KB, 
MAXSIZE = UNLIMITED, 
FILEGROWTH = 5242880 KB
) TO FILEGROUP Staging_TSTFG_03
GO

Use Staging_TST
go

CREATE PARTITION FUNCTION OrderPartitionFunction (Datetime) 
AS RANGE LEFT FOR VALUES ('20171201', '20180101'); 
GO

CREATE PARTITION SCHEME OrderPartitionScheme
AS PARTITION OrderPartitionFunction
TO (Staging_TSTFG_01,Staging_TSTFG_02,Staging_TSTFG_03);
GO

--Creating Orders table
CREATE TABLE Orders (
OrderID INT IDENTITY NOT NULL,
OrderDate DATETIME NOT NULL,
CustomerID INT NOT NULL, 
OrderStatus CHAR(1) NOT NULL DEFAULT 'P',
ShippingDate DATETIME
);
Go

ALTER TABLE Orders ADD CONSTRAINT PK_Orders PRIMARY KEY Clustered (OrderID, OrderDate)
ON OrderPartitionScheme (OrderDate);
Go

INSERT INTO [dbo].[Orders]([OrderDate],[CustomerID],[OrderStatus],[ShippingDate])
VALUES(DateAdd(d, ROUND(DateDiff(d, '2017-10-01', '2017-12-31') * RAND(CHECKSUM(NEWID())), 0),DATEADD(second,CHECKSUM(NEWID())%48000, '2017-10-01')),ABS(CHECKSUM(NewId())) % 1000,'P',DateAdd(d, ROUND(DateDiff(d, '2017-10-01', '2017-12-31') * RAND(CHECKSUM(NEWID())), 0),DATEADD(second,CHECKSUM(NEWID())%48000, '2017-10-01')))
GO 1000
			
      
Automate Sliding Window Partition Management: Part I

Hugh Scott, 2014-03-07 (first published: 2010-12-14)
Introduction

We implemented partitioned tables in one of our data marts primarily to facilitate purging data. Our business users defined a requirement to retain 4 years of data and that the data could be dropped in monthly increments. Prior experience dealing with SQL Server and large deletes led me to experiment with partitioning. I wasn't overly concerned with improving daily performance of bulk loads or building the Analysis Server cubes. I was concerned with the monthly requirement to purge hundreds of thousands, perhaps even millions of records. In addition, as we added new fact tables to the data mart, I did not want to add more purge routines. Using partitioning with a common partition scheme based on a date column, I am able to add new fact tables without changing the purge process at all.

I should note that all of this could have been done in T-SQL. There's no specific reason that I had to implement this using PowerShell. I looked at some other tools to facilitate managing partitions, but none of them quite fit the bill. Most were geared around maintaining a single table at a time or were GUI interfaces with no scriptable component.

I have to confess too that I seriously wanted to work with PowerShell as a scripting tool.

Finally, I have to give credit to Stuart Ozer. A huge chunk of this was adapted from his PartitionManager C# class library. That may have made the decision to work with PowerShell easier, since I could easily adapt some of those routines directly into the PowerShell code.

I first published this as set of two scripts on SQLServerCentral. The code pretty much stayed in that form until recently when I had some time to review and revise it. The biggest things that I wanted to fix from the original scripts that I posted:

    Allow the scripts to automatically calculate boundary dates for both splitting (adding a new partition) and merging (removing a trailing) partition.
    Add some error handling to more gracefully handle situations that I had not anticipated in production.
    From a published article standpoint, I also wanted to add a set up script to help admins who were not familiar with setting up partitioning.

I should note that I am NOT an expert in partitioning. Nor am I an expert coder. My understanding of partitioning continues to evolve and I continue to discover new ways to approach things and occasionally issues that cause me to head back to the books for more research.

So, without further ado, here goes...
Part I: Design and Setup

Planning and Design

Creating a sliding window partition strategy requires some planning and some resources. First off, you need SQL Server Enterprise Edition or Developer Edition. None of the other editions of SQL Server 2005 or 2008 support partitioning. SQL Server 2000 and earlier do not support partitioning at all.

Next, you need to decide which tables are going to be partitioned and which column you are going to use as the basis for your partitioning scheme. It's important that once you have identified the tables and the common field for partitioning that you pause and review your design and your data model for potential issues. If table A contains records that are related to table B, you want to be sure that the data in both tables "ages out" at the same time. You don't want to get into a situation where you are creating orphan records.

In our case, the choice was easy and was based on the activity close date which was more or less common to several fact tables. We have eight fact tables which all provide differing levels of detail regarding orders. The tables are related to one another by a surrogate key for the order. When an order "ages out", then all of the data related to that order can be aged out as well.

I cannot stress enough how important this design and review is. If you get this part wrong, you will spend many hours fixing it down the road. Design it. Sketch it out. Review it with the data owners. Review it with your developers. Then put it away for a night or two and come back to it when you have a clear, fresh perspective.
Partition Function

Your next step will be to create a partition function. The partition function will define the boundaries which divide the table into its constituent partitions. If you've done the design part well, this step will come pretty easily. The syntax for creating a partition function is:

CREATE PARTITION FUNCTION partition_function_name ( input_parameter_type )
AS RANGE [ LEFT | RIGHT ] 
FOR VALUES ( [ boundary_value [ ,...n ] ] )

From a snippet of the attached PartitionDBSetup.sql script, it looks like:

CREATE PARTITION FUNCTION [pf_FACT_DATA_DATE](datetime) AS RANGE RIGHT FOR 
VALUES (N'2008-10-01', N'2008-11-01', N'2008-12-01')

One note here about partition functions and boundary values. You will always have an upper boundary and a lower boundary. Values that are greater than the upper boundary will go into the "upper" partition. Values lower than the lower boundary will go into the "lowest" partition. If I've understood partitioning correctly, you can't really purge data from the lowest partition, since this partition always contains data less than the lowest boundary value. Essentially, if you have numbered your partitions 1, 2, 3 ... etc, you will always be purging data from partition number 2.

This is a key understanding on my part and is manifested in the MergePartition.ps1 script in way in which the boundary date is automatically calculated (when not specified as a parameter). We implemented our lowest partition as a deliberately "empty" partition (on a file group called "FACT_EMPTY" and the data file associated with it is deliberately small with auto grow turned off. If any data makes it into this partition, it's because of a data error or an issue with the ETL.
Partition Scheme

Next, we created a partition scheme. Again, this is pretty straightforward if you've done your homework. You simply need to have worked out how many boundaries you have in your partition function and add one. In our case, the business wants to keep four years of data by month, so we have 50 partitions and 49 boundaries. This gives us:

1 empty partition on the "lower" side; this one should never have data

48 "active" partitions

1 empty partition on the "upper" side; this is ready to store data on the first of the new month

The syntax for creating the partition scheme is:

CREATE PARTITION SCHEME partition_scheme_name
AS PARTITION partition_function_name
[ ALL ] TO ( { file_group_name | [ PRIMARY ] } [ ,...n ] )

Again, from a snippet of the attached PartitionDBSetup.sql script, it looks like:

CREATE PARTITION SCHEME [ps_FACT_DATA_DATE] AS PARTITION [pf_FACT_DATA_DATE] TO 
([FACT_EMPTY], [FACT_2008_M10], [FACT_2008_M11], [FACT_2008_M12])

Note here that there is one more partition than there are boundary values. Note also that the lower boundary is named [FACT_EMPTY]. It is not intended for data to be stored here. It's here for administrative purposes and (at least initially during development) helped us to identify data quality issues (null valued data fell out into the FACT_EMPTY partition).
Files and File Groups

One thing to consider here is whether you are going to split your data across multiple file groups (for better performance and IO optimization), or use a single file group. The scripts are really geared around using multiple file groups, but that's not an essential requirement. Remember, the overarching objective here was to facilitate purging of data; it's not all about performance.

Next we created the individual fact tables with the partition column. One key point: in order to perform the "magic" of the switch process, any indexes that you create on partitioned tables must be "storage aligned". This means that any index that you create must be created with the partitioned column. This makes primary keys and unique keys a challenge, especially when using date values for partitioning. Candidly, I have not overcome this particular challenge in our environment, although we do not appear to have an issue with duplicate data.

Here is a sample table excerpted from the accompanying script:

CREATE TABLE Orders (
 OrderCloseDate datetime not null, 
 OrderNum int not null, 
 [Status] char(2) null,
 CustomerID int not null)
ON ps_FACT_DATA_DATE (OrderCloseDate)
GO

CREATE INDEX IX_Orders_CustomerID
ON Orders (OrderCloseDate, CustomerID)
ON ps_FACT_DATA_DATE (OrderCloseDate)
GO

The included setup script creates an empty database, with the necessary file groups and data files. The script also creates two partitioned tables and some storage aligned indexes. The script contains insert statements to minimally populate each table with data. Finally, the script creates a view which helps you as an administrator to visualize the partitions and the amount of data held in each partition.

After running the entire script, you should be able to execute the following from a query window:

SELECT * FROM partition_info ORDER BY TableName, partitionNumber

Note the information provided and the number of rows for each table in each partition. Note also the first and last rows for each table, which are easily identified by the null values for LowerBoundary and UpperBoundary.
Conclusion

This introduction to partitioning and the included samples will hopefully clarify some of the concepts and give you a "sandbox" in which to try out some of the concepts related to partitioning with SQL Server. In the next article, I will discussion splitting a partition, which is used to add a new partition at the "top" end of the partition scheme.
      
