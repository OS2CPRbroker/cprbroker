
  
SELECT Y,M,TXT, COUNT(*)As Count from (
	SELECT 
		substring(cast(DPRAJDTO as varchar(20)),1,4) As Y, 
		substring(cast(DPRAJDTO as varchar(20)),5,2) As M,
		SUBSTRING(FEJLTXT,1,43) AS TXT
	FROM [DTABONNFEJL]
) as aaa
group by Y,M,TXT
order by Y,M,TXT

select top 10 * from [DTABONNFEJL]

