import time
import boto3
from typing import List, Any

client = boto3.client("athena")


def has_query_succeeded(execution_id: str) -> bool:
    state = "RUNNING"
    max_execution = 10

    while max_execution > 0 and state in ["RUNNING", "QUEUED"]:
        max_execution -= 1
        response = client.get_query_execution(QueryExecutionId=execution_id)
        if (
            "QueryExecution" in response
            and "Status" in response["QueryExecution"]
            and "State" in response["QueryExecution"]["Status"]
        ):
            state = response["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                return True

        time.sleep(10)

    return False


def create_database(database_name: str, output_location: str | None = None, workgroup: str = "primary") -> Any:

    if not output_location:
        response = client.start_query_execution(
            QueryString=f"CREATE DATABASE IF NOT EXISTS {database_name}",
            WorkGroup=workgroup
        )

        return response["QueryExecutionId"]

    response = client.start_query_execution(
        QueryString=f"CRETE DATABASE IF NOT EXISTS {database_name}",
        ResultConfiguration={
            "OutputLocation": output_location
        },
        WorkGroup=workgroup
    )

    return response["QueryExecutionId"]


def query_db(query: str, output_location: str | None = None, workgroup: str = "primary") -> Any:

    if not output_location:
        response = client.start_query_execution(
            QueryString=query,
            WorkGroup=workgroup
        )

        return response["QueryExecutionId"]

    response = client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            "OutputLocation": output_location
        },
        WorkGroup=workgroup
    )

    return response["QueryExecutionId"]


def get_query_results(execution_id: str) -> List[Any] | None:
    response = client.get_query_results(
        QueryExecutionId=execution_id
    )

    results = response['ResultSet'].get('Rows')
    return results


if __name__ == "__main__":
    from pprint import pprint

    boto3.setup_default_session(
        profile_name='spinyleaks-dev', region_name="us-east-1")
    client = boto3.client("athena")

    # result_id = create_database("cvm_data", workgroup="analytics-workgroup")

    # status = has_query_succeeded(result_id)

    # if not status:
    #     raise Exception("cvm_data failed to be created!!!")

    query = """
            CREATE EXTERNAL TABLE IF NOT EXISTS
                cvm_data.historico_cotas_fundos (
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
                'separatorChar' = '073',
                'quoteChar' = '\"',
                'escapeChar' = '\\'
                )
                STORED AS TEXTFILE
                LOCATION 's3://dev-leaksspiny-lake/raw/cvm/cotas-fundos/';
    """

    result_id = query_db(query, workgroup="analytics-workgroup")

    status = has_query_succeeded(result_id)

    if not status:
        raise Exception("fail to create table!!!")

    query = """
            SELECT
                CNPJ_FUNDO, avg(RESG_DIA)
            FROM
                cvm_data.historico_cotas_fundos
            GROUP BY
                CNPJ_FUNDO
            ;
            """

    result_id = query_db(query, workgroup="analytics-workgroup")

    status = has_query_succeeded(result_id)

    if not status:
        raise Exception("fail to query table!!!")

    result = get_query_results(result_id)

    pprint(result)
