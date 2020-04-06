/*
	Мёрдж данных из внешней таблицы во внутреннюю
	Мёрджим тупо по UUID потому что времени не вагон. 
	В продакшене конечно боже упаси такое делать =) 
	
	vsql -h34.71.139.131 -Udbadmin -f "D:\_git\andreev-ds-de-diploma\sql\SP_MERGE_OPTY_FROM_EXT_TO_INT.sql"
*/


MERGE INTO DED.OPTY TGT 
USING (
	SELECT * FROM (
		SELECT 
			UUID
			, SYSDATE LOAD_DT
			, CREATED - 1000 + RANDOM() * 1000 AS CREATED -- данные и так синтетические, добавим разброс на 3 года для отчетов 
			, REGION 
			, JOB_TITLE 
			, SALARY 
			, LOAN_AMOUNT
			, PERIOD
			, CASE WHEN TARGET = 1 THEN 'POS' ELSE 'NEG' END AS RES 
			, ROW_NUMBER() OVER (PARTITION BY UUID ORDER BY CREATED DESC) AS RN
		FROM DED.OPTY_EXTERNAL 
	) MAIN_SRC WHERE RN = 1
) SRC ON (TGT.UUID = SRC.UUID)
WHEN NOT MATCHED THEN INSERT (
	UUID
	, LOAD_DT
	, CREATED 
	, REGION 
	, JOB_TITLE 
	, SALARY 
	, LOAN_AMOUNT
	, PERIOD
	, RES 
) VALUES (
	SRC.UUID
	, SRC.LOAD_DT
	, SRC.CREATED 
	, SRC.REGION 
	, SRC.JOB_TITLE 
	, SRC.SALARY 
	, SRC.LOAN_AMOUNT
	, SRC.PERIOD
	, SRC.RES 
);

COMMIT;


