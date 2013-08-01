IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ttable]') AND type in (N'U'))
BEGIN
CREATE TABLE [ttable](
	[pk] [bigint] IDENTITY(1,1) NOT NULL,
	[cbool] [bit] NULL,
	[cint] [int] NULL,
	[cfloat] [float] NULL,
	[cnumeric] [numeric](10, 4) NULL,
	[cstring] [nvarchar](100) NULL,
	[cdate] [smalldatetime] NULL,
	[cdatetime] [datetime] NULL,
 CONSTRAINT [PK_ttable] PRIMARY KEY CLUSTERED 
(
	[pk] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
)
END
GO

create procedure [usp_query](@cint int)
as
begin
	select * from ttable where cint > @cint;
end;
GO


create procedure [usp_exec](@cint int)
as
begin
	 delete from ttable where cint = @cint;  
end;
GO


create procedure [usp_inout](@x int, @y int output, @sum int output)
as
begin
	set @sum = @x + @y;
	set @y = 2 * @y
end;
GO

