MSCK REPAIR TABLE cvm_data.historico_costas_fundos;
DESCRIBE FORMATTED cvm_data.historico_cotas_fundos;
DROP TABLE IF EXISTS cvm_data.historico_cotas_fundos;
SELECT *
FROM cvm_data.historico_cotas_fundos_1
where nr_cotst = ?;

CREATE EXTERNAL TABLE
IF NOT EXISTS
  cvm_data.historico_cotas_fundos_1
(
  TP_FUNDO string,
  CNPJ_FUNDO string,
  DT_COMPTC string,
  VL_TOTAL float,
  VL_QUOTA float,
  VL_PATRIM_LIQ float,
  CAPTC_DIA float,
  RESG_DIA float,
  NR_COTST int
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ';'
  ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3://dev-leaksspiny-lake/raw/cvm/cotas-fundos/'
tblproperties
("skip.header.line.count"="1");
;


CREATE EXTERNAL TABLE
IF NOT EXISTS
  cvm_data.historico_cotas_fundos_2
(
  TP_FUNDO string,
  CNPJ_FUNDO string,
  DT_COMPTC string,
  VL_TOTAL float,
  VL_QUOTA float,
  VL_PATRIM_LIQ float,
  CAPTC_DIA float,
  RESG_DIA float,
  NR_COTST int
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://dev-leaksspiny-lake/raw/cvm/cotas-fundos/'
tblproperties
("skip.header.line.count"="1");
;

CREATE TABLE
IF NOT EXISTS cvm_data.cotas_fundos_opt
WITH
(
    format = 'Parquet',
    write_compression = 'SNAPPY',
    external_location = 's3://dev-leaksspiny-lake/raw/cvm/fundos-cotas/',
    partitioned_by = ARRAY ['anomes']
)
AS
SELECT
  TP_FUNDO,
  CNPJ_FUNDO,
  DT_COMPTC,
  VL_TOTAL,
  VL_QUOTA,
  VL_PATRIM_LIQ,
  CAPTC_DIA,
  RESG_DIA,
  NR_COTST,
  CONCAT(SUBSTR(DT_COMPTC, 1, 4),SUBSTR(DT_COMPTC, 6, 2)) AS ANOMES
FROM
  cvm_data.historico_cotas_fundos_1
;


CREATE TABLE
IF NOT EXISTS cvm_data.cotas_fundos_bucket
WITH
(
    format = 'Parquet',
    write_compression = 'SNAPPY',
    external_location = 's3://dev-leaksspiny-lake/raw/cvm/fundos-bucket/',
    partitioned_by = ARRAY ['anomes'],
    bucketed_by = ARRAY ['cnpj_fundo'],
    bucket_count = 5
)
AS
SELECT
  TP_FUNDO,
  CNPJ_FUNDO,
  DT_COMPTC,
  VL_TOTAL,
  VL_QUOTA,
  VL_PATRIM_LIQ,
  CAPTC_DIA,
  RESG_DIA,
  NR_COTST,
  CONCAT(SUBSTR(DT_COMPTC, 1, 4),SUBSTR(DT_COMPTC, 6, 2)) AS ANOMES
FROM
  cvm_data.historico_cotas_fundos_1
;

