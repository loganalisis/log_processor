from rest_framework.decorators import api_view, parser_classes
from rest_framework.parsers import MultiPartParser
from rest_framework.response import Response
from .kafka_client import publish_to_kafka
from azure.storage.blob import BlobClient
import os, uuid
import tempfile

account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")
container_name = os.environ.get("AZURE_STORAGE_CONTAINER")

connection_string = "DefaultEndpointsProtocol=https;AccountName=subhamstoragelogs;AccountKey=lNduVfrpwSM48iJbJNz/PDIxAG39J01lEt/HoWiLxT5/x5cbE6cCw/nZ0WD03NCPHuN/ZCv+ddys+AStNsn0ww==;EndpointSuffix=core.windows.net"
    # Specify your container name and the local and blob file names
container_name = "loguploadblob"

@api_view(['POST'])
@parser_classes([MultiPartParser])
def upload_log(request):
    file_obj = request.data['file']
    unique_name = f"{uuid.uuid4()}_{file_obj.name}"

    blob_client = BlobClient.from_connection_string(
        conn_str=connection_string,
        container_name=container_name,
        blob_name=unique_name
    )

    # blob_client.upload_blob(file_obj, overwrite=True)
    blob_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{unique_name}"
    

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        for chunk in file_obj.chunks():
            temp_file.write(chunk)
        temp_file_path = temp_file.name

    with open(temp_file_path, 'r') as f:
        for line_num, line in enumerate(f):
            if line.strip():
                msg = {
                    "file_name": file_obj.name,
                    "line_number": line_num + 1,
                    "content": line.strip(),
                    "blob_url": blob_url,
                }
                publish_to_kafka("logs_item2.uploaded", msg, unique_name)
                # print(f"Publishing to Kafka: {msg}")

    # Send metadata to Kafka
    # publish_to_kafka("logs.uploaded", {"file": unique_name, "url": blob_url})
    # publish_to_kafka("logs.uploaded", "hello")

    return Response({
        "message": "Uploaded to Azure Blob Storage",
        "unique_name": unique_name,
        "url": blob_url
    })
