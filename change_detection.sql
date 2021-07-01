-- FUNCTION: arpit_test.change_detection(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying)

-- DROP FUNCTION arpit_test.change_detection(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying);

CREATE OR REPLACE FUNCTION arpit_test.change_detection(
	schema_name character varying,
	schema_name_1 character varying,
	table_name character varying,
	table_name_1 character varying,
	database_name character varying,
	host character varying,
	unqid_col_name character varying,
	user_id character varying)
    RETURNS integer
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

DECLARE 
DECLARE returnstatus varchar;
DECLARE f1 text; f2 text;
DECLARE p1 text; p2 text;
DECLARE SQLQuery varchar;
DECLARE sqlq varchar;
DECLARE error_tab_name varchar;
DECLARE v_cnt varchar;
DECLARE ROW_COUNT varchar;
DECLARE update_log_tab_name varchar;
DECLARE table_count integer;
DECLARE SQLQuery1  varchar;
DECLARE query character varying;
DECLARE query_1 character varying;
DECLARE query_2 character varying;

BEGIN

	returnstatus =1 ;
	
	error_tab_name = ''||schema_name||'.error';
	
 SQLQuery =('select count(*) from(select column_name,data_type from information_schema.columns where table_name='''|| table_name ||''' and table_schema='''||schema_name||'''
			 except
			 select column_name,data_type from dblink(''dbname= '||database_name||'
													   port= 5432
													   host = '||host||'
													   user = postgres
													   password = postgres'', 
			 ''select column_name,data_type from information_schema.columns where table_name='''''|| table_name_1||''''' and table_schema='''''||schema_name_1||''''''')					 
			  as tab1 (column_name character varying, data_type character varying)				 
			  union all				
			  select column_name,data_type from information_schema.columns where table_name='''|| table_name_1 ||''' and table_schema='''||schema_name_1||'''
			  except
			  select column_name,data_type from dblink(''dbname= '||database_name||'
													   port= 5432
													   host ='||host||'
													   user = postgres
													   password = postgres'',
			  ''select column_name,data_type from information_schema.columns where table_name='''''|| table_name||'''''and table_schema='''''||schema_name||''''''')					 
			   as tab1 (column_name character varying, data_type character varying))a');
               
			   --RAISE INFO '<-----sql-----> %', SQLQuery;
     
	 EXECUTE SQLQuery into table_count;
     RAISE INFO 'COUNT:%',table_count ;

IF table_count=0 THEN
  
	sqlq='select array_to_string(array_agg(''"''||column_name||''"''),'','') from information_schema.columns where table_name='''||table_name||''' and table_schema='''||schema_name||'''';
	--RAISE INFO '<----sql----> %', sqlq;
	
	execute sqlq into query;
	--RAISE INFO '<----sql----> %', query;

	sqlq='select array_to_string(array_agg(''"''||column_name||''"''),'','') from information_schema.columns where table_name='''||table_name_1||''' and table_schema='''||schema_name_1||'''';
	--RAISE INFO '<----sql----> %', sqlq;
	
	execute sqlq into query_1;
	--RAISE INFO '<----sql----> %', query_2;

	sqlq='select array_to_string(array_agg(''"''||column_name||''"''||'' ''||case when data_type=''USER-DEFINED'' then ''geometry'' else data_type end),'','') from information_schema.columns where table_name='''||table_name_1||''' and table_schema='''||schema_name_1||'''';
	--RAISE INFO 'sql-> %', sqlq;
	
	execute sqlq into query_2;
	--RAISE INFO '<----sql-----> %', query_1;
	 
	sqlq= 'drop table if exists '||schema_name||'.change_details_'||USER_ID||'';
	execute sqlq;
	--RAISE INFO 'sql-> %', sqlq;
	
		sqlq =format('create table '||schema_name||'.change_details_'||USER_ID||' as (
				select row_number() over(order by tab."'||unqid_col_name||'") as "uid", * from
					(With tab1 As ( Select  '||query||' from '||schema_name||'."'|| table_name ||'" )
				select "'||unqid_col_name||'",
						''data updated''
				as REMARKS,
				skeys(hstore(t1)-hstore(t2)) "base_field_name",
				svals(hstore(t1)-hstore(t2)) "base_field_value",
				skeys(hstore(t2)-hstore(t1)) "compare_field_name",
				svals(hstore(t2)-hstore(t1))  "compare_field_value",
				current_timestamp as "created_datetime"
				from tab1  t1 full outer join  (select  '||query_1||' from dblink('' dbname= '||database_name||'
																				 port= 5432
																				 host = '||host||'
																				 user = postgres
																				 password = postgres'',
												 ''select '||query_1||'
				from '||schema_name_1||'."'|| table_name_1 ||'"  '') as tab2 ('||query_2||'))  t2 
							using("'||unqid_col_name||'")
				where t1 is distinct from t2 and t1."'||unqid_col_name||'" is not null and t2."'||unqid_col_name||'" is not null) as tab)');

				--RAISE INFO '<-------> Table created '||schema_name_1||'.change_details_'||USER_ID||'';
				
				--RAISE INFO '<-----sql-----> %', sqlq;
				EXECUTE sqlq;
			
	sqlq= 'drop table if exists '||schema_name||'.change_report_'||USER_ID||'';
	execute sqlq;
	--RAISE INFO 'sql-> %', sqlq;
		 
	sqlq =format('create table '||schema_name||'.change_report_'||USER_ID||' as(
				 select row_number() over(order by tab."'||unqid_col_name||'") as "uid", * from
				(With tab1 As ( Select  '||query||' from '||schema_name||'."'|| table_name ||'" )
					select "'||unqid_col_name||'",
				case 
		      		when t1."'||unqid_col_name||'" is null then ''deleted in table''
			  		when t2."'||unqid_col_name||'" is null then ''added in table''
				else
			   		 ''data updated''
						end as REMARKS,
			 	current_timestamp as "created_datetime"
				from tab1  t1 full outer join  (select  '||query_1||' from dblink('' dbname= '||database_name||'
							 port= 5432
							 host = '||host||'
							 user = postgres
							 password = postgres'',
				''select '||query_1||'
									from '||schema_name_1||'."'|| table_name_1 ||'"  '') as tab2 ('||query_2||'))  t2 
											using("'||unqid_col_name||'")
											where t1 is distinct from t2 )as tab)');
			
				--RAISE INFO '<--------> Table created '||schema_name_1||'.change_report_'||USER_ID||'';
				--RAISE INFO 'sql-> %', sqlq;
				EXECUTE sqlq;
			
				--ALTER SEQUENCE arpit_test.log_report_database_uid_seq restart;
				--update arpit_test.log_report_database set uid = nextval('arpit_test.log_report_database_uid_seq');
				--RAISE INFO '<-------> Changed Sequence Number';
			
ELSE
		RAISE INFO 'Table is not matched';
		SQLQuery =('INSERT INTO arpit_test.log_value_1(table_name,column_name,data_type) 
				   select '''|| table_name ||'''as table_name, column_name, data_type from
			(select column_name,data_type from information_schema.columns where table_name='''|| table_name ||''' and table_schema='''||schema_name||'''
			 except
			 select column_name,data_type from dblink(''dbname= '||database_name||'
													    port= 5432
													    host = '||host||'
													    user = postgres
													    password = postgres'', 
			''select column_name,data_type from information_schema.columns where table_name='''''|| table_name_1||''''' and table_schema='''''||schema_name_1||''''''')					 
			 as tab1 (column_name character varying, data_type character varying)				 
			 union all				
			 select column_name,data_type from information_schema.columns where table_name='''|| table_name_1 ||''' and table_schema='''||schema_name_1||'''
			 except
			 select column_name,data_type from dblink(''dbname= '||database_name||'
													   port= 5432
													   host ='||host||'
													   user = postgres
													   password = postgres'',
			''select column_name,data_type from information_schema.columns where table_name='''''|| table_name||'''''and table_schema='''''||schema_name||''''''')					 
						as tab1 (column_name character varying, data_type character varying))a');
   
		--RAISE INFO 'sql-> %', SQLQuery;
		
		EXECUTE SQLQuery;
		RAISE INFO '<-------->insert into LOG';
  
				--ALTER SEQUENCE arpit_test.log_value_id_seq restart;
				--update arpit_test.log_value set id = nextval('arpit_test.log_value_id_seq');
				--RAISE INFO '<-------> Changed Sequence Number';
		 
		 returnstatus = -1;
		
	end if;
		
	 return returnstatus;
		
EXCEPTION
	WHEN OTHERS THEN
	GET STACKED DIAGNOSTICS 
		f1=MESSAGE_TEXT,
		f2=PG_EXCEPTION_CONTEXT; 
		RAISE info 'error caught:%',f1;
		RAISE info 'error caught:%',f2;
		SQLQuery = FORMAT('INSERT INTO %1$s (table_name,table_schema,message,context) Values(''%2$s'',''%3$s'',''%4$s'',''%5$s'')',error_tab_name,table_name,schema_name,f1,f2);
		RAISE INFO '<---------> Data inserted in error table';
		--RAISE INFO 'sql-> %', SQLQuery;
		EXECUTE SQLQuery;
		 returnstatus=-1;

END 

$BODY$;

ALTER FUNCTION arpit_test.change_detection(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying)
    OWNER TO postgres;
