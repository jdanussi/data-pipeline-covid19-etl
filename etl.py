# Imports
import pandas as pd
from sqlalchemy import create_engine
import os
import zipfile
import boto3


def getParameter(param_name):
    """
    This function reads a secure parameter from AWS' SSM service.
    The request must be passed a valid parameter name, as well as 
    temporary credentials which can be used to access the parameter.
    The parameter's value is returned.
    """
    # Create the SSM Client
    ssm = boto3.client('ssm',
        region_name = AWS_REGION
    )

    # Get the requested parameter
    response = ssm.get_parameters(
        Names=[
            param_name,
        ],
        WithDecryption=True
    )
    
    # Store the credentials in a variable
    credentials = response['Parameters'][0]['Value']

    return credentials


def download_from_aws(bucket, s3_file, local_file):
    s3 = boto3.client('s3',
        region_name = AWS_REGION,
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
        aws_session_token = AWS_SESSION_TOKEN
        )

    try:
        s3.download_file(bucket, s3_file, local_file)
        print(f"Download {s3_file} Successful")
        return True
    except FileNotFoundError:
        print(f"The file {s3_file} was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


# Setting up variables
print("Setting up environment variables")
AWS_REGION = 'us-east-1'
S3_BUCKET = getParameter('/cde/S3_BUCKET_DATASETS')
DB_HOST = getParameter('/cde/DB_HOST')
DB_DATABASE = getParameter('/cde/DB_DATABASE')
DB_USER = getParameter('/cde/DB_USER')
DB_PASS = getParameter('/cde/DB_PASS')
DB_PORT = str(getParameter('/cde/DB_PORT'))


# Get AWS credentials from ECS intance profile
print("Getting up AWS credentials from ECS intance profile")
session = boto3.Session(region_name=AWS_REGION)
credentials = session.get_credentials()
credentials = credentials.get_frozen_credentials()
AWS_ACCESS_KEY_ID = credentials.access_key
AWS_SECRET_ACCESS_KEY = credentials.secret_key
AWS_SESSION_TOKEN = credentials.token


download_from_aws(S3_BUCKET, 'provincias.csv', 'data/provincias.csv')
download_from_aws(S3_BUCKET, 'departamentos.csv', 'data/departamentos.csv')
download_from_aws(S3_BUCKET, 'Covid19Casos.zip', 'data/Covid19Casos.zip')

with zipfile.ZipFile('data/Covid19Casos.zip', 'r') as zip_ref:
    print('Deflating Covid19Casos.zip ...')
    zip_ref.extractall('/etl/data')


# Instantiate sqlachemy.create_engine object
print("Instantiating sqlachemy.create_engine object")
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}')

# Truncate tables before load from csv
print("Truncating tables before load data from csv")
with engine.connect() as connection:
    connection.execute("""
    TRUNCATE TABLE state cascade;
    """)
    connection.execute("""
    TRUNCATE TABLE department cascade;
    """)
    connection.execute("""
    TRUNCATE TABLE covid19_case cascade;
    """)


# Read from csv "provincias.csv"
print("Reading from 'provincias.csv'")
df = pd.read_csv('/etl/data/provincias.csv')

print("Transforming data")
# Select a subset of columns
columns = ['id', 'nombre']
df = pd.DataFrame(df, columns=columns)

# Rename columns
df. rename(columns = {
    'id':'state_id', 
    'nombre':'state_name'}, 
    inplace = True)

# Drop rows with na values in the PK
df.dropna(subset=['state_id'], inplace=True)

# Drop rows for duplicates values in the PK
df.drop_duplicates(subset=['state_id'], inplace=True)

# Replace some long state name for shorter versions
df.loc[ df['state_id'] == 94, 'state_name'] = 'Tierra del Fuego'
df.loc[ df['state_id'] == 2, 'state_name'] = 'CABA'

# Save the data from dataframe to postgres table "state"
print("Loading data to database")
df.to_sql(
    'state', 
    engine,
    schema='public',
    index=False,
    if_exists='append'
)


# Read from csv "departamentos.csv'"
print("Reading from 'departamentos.csv'")
df = pd.read_csv('/etl/data/departamentos.csv')

print("Transforming data")
# Select a subset of columns
columns = ['id', 'nombre', 'provincia_id']
df = pd.DataFrame(df, columns=columns)

# Rename columns
df. rename(columns = {
    'id':'department_id', 
    'nombre':'department_name', 
    'provincia_id':'state_id'},
    inplace = True)

# Drop rows with na values in the PK
df.dropna(subset=['department_id'], inplace=True)

# Drop rows for duplicates values in the PK
df.drop_duplicates(subset=['department_id'], inplace=True)

# Save the data from dataframe to postgres table "department"
print("Loading data to database")
df.to_sql(
    'department', 
    engine,
    schema='public',
    index=False, 
    if_exists='append' 
)


# Iterable to read "chunksize=30000" rows at a time from the CSV file

columns = [
    'id_evento_caso', 'sexo', 'edad', 'fecha_inicio_sintomas', 'fecha_apertura',
    'fecha_fallecimiento', 'asistencia_respiratoria_mecanica', 'carga_provincia_id',
    'clasificacion_resumen', 'residencia_provincia_id', 'fecha_diagnostico',
    'residencia_departamento_id', 'ultima_actualizacion']


# Read from csv "Covid19Casos.csv"
print("Reading from 'Covid19Casos.csv'")
print("Transforming and loading data to database")
for df in pd.read_csv('/etl/data/Covid19Casos.csv', chunksize=30000):

    # Select a subset of columns
    df = pd.DataFrame(df, columns=columns)

    # Rename columns
    df.rename(columns = {
        'id_evento_caso':'covid_case_csv_id',
        'sexo':'gender_id',
        'edad':'age',
        'fecha_inicio_sintomas':'symptoms_start_date',
        'fecha_apertura':'registration_date',
        'fecha_fallecimiento':'death_date',
        'asistencia_respiratoria_mecanica':'respiratory_assistance',
        'carga_provincia_id':'registration_state_id', 
        'clasificacion_resumen':'clasification',
        'residencia_provincia_id':'residence_state_id',
        'fecha_diagnostico':'diagnosis_date', 
        'residencia_departamento_id':'residence_department_id', 
        'ultima_actualizacion':'last_update'},
        inplace = True)
    
    try:
        # Drop rows with na values in "covid_case_csv_id" column
        df.dropna(subset=['covid_case_csv_id'], inplace=True)

        # Select only rows for the current year
        df.drop(df[df.registration_date.str[:4] != '2022'].index,  inplace = True)

        # Drop rows for duplicates values in "covid_case_csv_id" column
        df.drop_duplicates(subset=['covid_case_csv_id'], inplace=True)

        # Drop rows "registration_state_id" = 0 (not in "state" table)
        df.drop(df[df['registration_state_id'] == 0].index, inplace = True)

        # Drop rows without "age"
        df.dropna(subset=['age'], inplace=True)
    
        # Change the type of "age" column to integer
        df['age'] = df['age'].astype('int64')
    
        # Save the data from dataframe to postgres table "covid19_case"
        df.to_sql(
            'covid19_case', 
            engine,
            schema='public',
            index=False,
            if_exists='append' 
            )
    except:
        raise


# Delete duplicates on "covid_case_csv_id"
print("Deleting duplicates on 'covid_case_csv_id'")
with engine.connect() as connection:
    connection.execute("""
    DELETE FROM covid19_case a USING (
      SELECT MIN(covid_case_id) as covid_case_id, covid_case_csv_id
        FROM covid19_case 
        GROUP BY covid_case_csv_id HAVING COUNT(*) > 1
      ) b
      WHERE a.covid_case_csv_id = b.covid_case_csv_id
      AND a.covid_case_id <> b.covid_case_id;
    """)