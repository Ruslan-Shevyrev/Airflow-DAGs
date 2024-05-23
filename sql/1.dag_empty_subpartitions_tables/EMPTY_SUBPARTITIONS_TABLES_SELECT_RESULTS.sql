WITH
FUNCTION with_function_num_rows(p_owner				IN VARCHAR2
								,p_table			IN VARCHAR2
								,p_part				IN VARCHAR2
								,p_seg_type			IN VARCHAR2
								,p_parallel_degree	IN NUMBER)

RETURN NUMBER
IS
	v_bar      number :=0;
	v_sql_text VARCHAR(4000);
BEGIN
	IF p_seg_type='TABLE PARTITION' THEN
		v_sql_text := 'SELECT  /*+ PARALLEL(t, '
					||p_parallel_degree
					||') FULL(t) */ count(0) FROM '
					||p_owner
					||'.'
					||p_table
					||' partition('
					||p_part
					||') t WHERE rownum=1';
	ELSIF p_seg_type='TABLE SUBPARTITION' THEN
		v_sql_text := 'SELECT /*+ PARALLEL(t, '
					||p_parallel_degree
					||') FULL(t) */ count(0) FROM '
					||p_owner
					||'.'
					||p_table
					||' subpartition('
					||p_part
					||') t WHERE rownum=1';
	ELSIF p_seg_type='TABLE' THEN
		v_sql_text := 'SELECT /*+ PARALLEL(t, '
			||p_parallel_degree
			||') FULL(t) */ count(0) FROM '
			||p_owner
			||'.'
			||p_table
			||' t WHERE rownum=1';
	END IF;

	EXECUTE IMMEDIATE v_sql_text INTO v_bar;
	RETURN v_bar;
EXCEPTION WHEN OTHERS THEN
	RETURN 1;
END;
	SELECT *
		FROM(SELECT dbs.owner
					,dbs.segment_name
					,dbs.partition_name
					,dbs.segment_type
					,TO_NUMBER(with_function_num_rows(dbs.owner,
														dbs.segment_name,
														dbs.partition_name,
														dbs.segment_type,
														4
														)) AS num_rows

					,ROUND(dbs.bytes/1024/1024)   segment_size_Mb
					,ROUND((SELECT sum(bytes/1024/1024)
								FROM dba_segments
								WHERE segment_name IN (SELECT index_name
															FROM dba_indexes dbi
															WHERE dbi.table_owner = dbs.owner
																AND dbi.table_name = dbs.segment_name
																AND dbs.segment_type = 'TABLE'))) AS index_size_for_Tables

				FROM dba_segments dbs
				WHERE 1=1
					AND dbs.segment_type IN ('TABLE PARTITION'
											,'TABLE SUBPARTITION'
											,'TABLE')
					AND dbs.bytes/1024/1024 >100
					)
		WHERE
			num_rows=0
		ORDER BY 5
				, segment_size_Mb DESC