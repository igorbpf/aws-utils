import boto3

textract_client = boto3.client("textract")


def start_job(bucket_name: str, object_name: str):
    response = textract_client.start_document_analysis(
        DocumentLocation={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': object_name
            }
        },
        FeatureTypes=['TABLES', 'FORMS']
    )

    return response["JobId"]


def get_job_results(job_id, next_token=None):
    kwargs = {}

    if next_token:
        kwargs['NextToken'] = next_token

    response = textract_client.get_document_analysis(JobId=job_id, **kwargs)

    return response
