import requests
import json
import boto3
import time
import os

# Credenciales
API_KEY = os.getenv('API_KEY')
API_URL = os.getenv('API_URL')
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_ACCESS_KEY = os.getenv('SECRET_ACCESS_KEY')


# Método que se encarga de usar datos de la api
def lambda_handler(event, context):
    # message = event['Records'][0]['Sns']['Message']
    # json_message = json.loads(message)
    # function = json_message['function']
    # codigo = json_message['codigo']
    # categoria = json_message['categoria']

    url = API_URL

    # Petición a la ruta
    # querystring = {"codigo": codigo, "categoria": categoria}

    # Extraemos la URL de la api y se consume por medio del request
    # GET y eso nos da una data de tipo json
    data = requests.request("GET", url, headers={"$$app_token": API_KEY}).json()
    print('data ', data)

    date_consult_api = int(time.time())
    if len(data) == 0:
        print('data not found')  # No recibió ninguna data
    else:
        if len(data) >= 0:
            # Se utiliza para generar un diccionario de datos el cual va hacer
            # utilizado para mandar los datos a S3
            data_dict = {
                'data_api': data,
                'date_consult_api': data,  # duda para mañana
                'date_load_data': time.time(),
            }

            # Éste método recibe el data que se generó en el data diccionario
            response_upload = uploadS3(data_dict)
            if response_upload:
                print('la data se cargo correctamente')
                # lanzar la otra lambda

            else:
                print('la data no se cargo correctamente')


# Método que carga los datos al S3, dándole una ruta del bucket y una ruta del load key
def uploadS3(data):
    try:
        # Guardamos el S3 en el bucket de extraer y le enviamos la ruta del archivo
        destination_s3_bucket = 'extractbut'
        upload_file_key = 'dataextrae'
        filepath = upload_file_key + ".json"  # Extensión del archivo que se carga al S3

        s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY,
                                 region_name='us-east-1')  # Credenciales de aws y region name por defecto
        response = s3_client.put_object(
            Bucket=destination_s3_bucket, Key=filepath, Body=(bytes(json.dumps(data).encode('UTF-8')))
        )

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            return True
        else:
            return False

    except Exception as e:
        print("Data load error: " + str(e))


# Método que se encarga de que una vez se ejecute la lambda de extraer la termine
# y posteriormente continue con la siguiente lambda
def run_lambda(codigo, categoria):
    topicArn = 'arn:aws:lambda:us-east-1:174375984229:function:transformar'  # ARN de la función para conectarnos al SNS
    snsClient = boto3.client(
        'sns',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_ACCESS_KEY,
        region_name='us-east-1'
    )
    publish0ject = {"function": "transformar", "codigo": str(codigo), "categoria": str(categoria)}
    response = snsClient.publish(TopicArn=topicArn,
                                 Message=json.dumps(publish0ject),
                                 Subject='FUNCTION',
                                 MessageAttributes={
                                     "TransactionType": {"DataType": "String", "StringValue": "FUNCTION"}})

    status = response['ResponseMetadata']['HTTPStatusCode']
    if status == 200:
        return True
    else:
        return False
