import boto3
# Exporto el archivo de una ubicacion de la pc a s3 extractbut
s3 = boto3.client('s3')
s3.upload_file('C:/Users/Lenovo/Documents/S12/BI/ProyectoFinal/PF2.0/data.json', 'extractbut', 'data.json')
