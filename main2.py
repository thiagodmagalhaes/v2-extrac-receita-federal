import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import re
import os
import zipfile
import boto3
import time
from botocore.exceptions import NoCredentialsError
from io import BytesIO


load_dotenv()

#load_dotenv(dotenv_path='./.env.windows', override=True)
CAMINHO_PASTA = os.path.dirname(os.path.abspath(__file__)) #CAMINHO DA PASTA RAIZ DO PROJETO
URL_RF = 'http://200.152.38.155/CNPJ/dados_abertos_cnpj/'
CONTADOR_EMPRESAS = 0
CONTADOR_ESTABELECIMENTOS = 1 


def autenticacao_aws():
    s3_client = boto3.client(
        's3',
        aws_access_key_id='',
        aws_secret_access_key=''
        #region_name='REGIAO'
    )
    return s3_client

def upload_to_s3(file_name, bucket, object_name=None):
    s3_client = boto3.client(
        's3',
        aws_access_key_id='',
        aws_secret_access_key=''
        # region_name='REGIAO'
    )
    # Se object_name não for especificado, o nome do arquivo será usado
    if object_name is None:
        object_name = file_name

    try:
        # Faz o upload do arquivo
        s3_client.upload_file(file_name, bucket, object_name)
        print(f"Arquivo {file_name} carregado com sucesso para o bucket {bucket} como {object_name}.")
    except FileNotFoundError:
        print("O arquivo não foi encontrado.")
    except NoCredentialsError:
        print("Credenciais AWS não encontradas.")
        
import boto3

def excluir_arquivos(diretorio):
    s3_client = autenticacao_aws()
    bucket_name = 'abrasel-datalake'  # Nome do bucket correto

    # Lista todos os objetos dentro do diretório 'raw/CNPJ/cnpj_simples'
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=diretorio)

    # Verifica se há objetos a serem excluídos
    if 'Contents' in response:
        for obj in response['Contents']:
            # Exclui cada objeto encontrado
            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
            print(f"Arquivo excluído: {obj['Key']}")
    else:
        print("Nenhum arquivo encontrado para excluir.")

# Função para obter os links da última atualização
def obter_links_ultima_atualizacao(URL_RF):
    try:
        response = requests.get(URL_RF)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = []
        
        for link in soup.find_all('a'):
            href = link.get('href')
            if href and re.match(r'\d{4}-\d{2}', href):
                links.append(href)

        links_organizados = [link.strip('/') for link in links]
        links_dict = {i + 1: f"{URL_RF}{link}" for i, link in enumerate(links_organizados)}
        return max(links_dict.values())
    
    except:
        print('\nNão foi possível obter o link atualizado.\nTentando novamente.')
        obter_links_ultima_atualizacao(URL_RF)


def baixar_empresas(link_ultima_atualizacao,CONTADOR_EMPRESAS):
    if(CONTADOR_EMPRESAS<10):
        try:
            zip_url = link_ultima_atualizacao+'/Empresas{}.zip'.format(CONTADOR_EMPRESAS)
            print('Baixando arquivo EMPRESAS nº {}'.format(CONTADOR_EMPRESAS))
            response = requests.get(zip_url, timeout=220)
            
            if response.status_code == 200:
                print('Download efetuado com sucesso!')
                zip_data = BytesIO(response.content)
                extrair_empresas(zip_data, CONTADOR_EMPRESAS)
                CONTADOR_EMPRESAS = CONTADOR_EMPRESAS+1
                baixar_empresas(link_ultima_atualizacao,CONTADOR_EMPRESAS)
                
            else:
                print('ERRO: Houve um instabilidade no download.\nTentando download novamente.')
                baixar_empresas(link_ultima_atualizacao,CONTADOR_EMPRESAS)
            
        except requests.exceptions.Timeout:
            print('ERRO: Tempo limite excedido para o download.\nTentando download novamente.')
            baixar_empresas(link_ultima_atualizacao, CONTADOR_EMPRESAS)
            
        except Exception as e:
            print('ERRO: Não foi possível baixar EMPRESAS. Verifique a conexão com LINK {}.\nTentando download novamente.'.format(zip_url, CONTADOR_EMPRESAS))
            baixar_empresas(link_ultima_atualizacao,CONTADOR_EMPRESAS)
            
def extrair_empresas(zip_data, CONTADOR_EMPRESAS):
    try:
        print('Extraindo arquivo...')
        with zipfile.ZipFile(zip_data) as zip_ref:
            zip_ref.extractall(CAMINHO_PASTA)
            print('Arquivo nº {} extraido com sucesso!'.format(CONTADOR_EMPRESAS))
    except:
        print('ERRO: Não foi possível extrair arquivo nº{}.\nTentando extrair novamente . . .'.format(CONTADOR_EMPRESAS))
        extrair_empresas(zip_data, CONTADOR_EMPRESAS)


def enviar_empresas_aws():
    i = 0
    diretorio = 'raw/CNPJ/cnpj_empresas/'
    excluir_arquivos(diretorio)
    print('Enviando enviar arquivos .EMPRECSV para AWS . . .')
    for nome_arquivo in os.listdir(CAMINHO_PASTA):
        if nome_arquivo.endswith(".EMPRECSV"):
            caminho_antigo = os.path.join(CAMINHO_PASTA, nome_arquivo)
            novo_nome = "EMPRESAS{}.EMPRECSV".format(i)
            time.sleep(5)
            caminho_novo = os.path.join(CAMINHO_PASTA, novo_nome)
            os.rename(caminho_antigo, caminho_novo)
            file_name = caminho_novo
            bucket_name = 'abrasel-datalake'  # Nome do bucket correto
            object_name = 'raw/CNPJ/cnpj_empresas/'+novo_nome  # Caminho dentro do bucket
            time.sleep(5)
            upload_to_s3(file_name, bucket_name, object_name)
            i = i+1
            
    print('Arquivos EMPRESAS enviados com sucesso!')

def baixar_cnaes(link_ultima_atualizacao):
    try:
        cnaes_url = link_ultima_atualizacao+'/cnaes.zip'
        print('\nBaixando CNAES . . .')
        response = requests.get(cnaes_url)
        
        if response.status_code == 200:
            print('\nDownload efetuado com sucesso!')
            zip_data = BytesIO(response.content)
            extrair_um_arquivo(zip_data)
        else:
            print('ERRO: Houve uma instabilidade no download.')
            baixar_cnaes(link_ultima_atualizacao)
            
    except:
        print('ERRO: Não foi possível baixar CNAE. Verifique a conexão com LINK {}'.format(cnaes_url))
        baixar_cnaes(link_ultima_atualizacao)        

def enviar_cnaes_aws():
    diretorio = 'raw/CNPJ/cnpj_cnae/'
    excluir_arquivos(diretorio)
    print('Enviando arquivos .CNAECSV para AWS . . .')
    
    for nome_arquivo in os.listdir(CAMINHO_PASTA):
        if nome_arquivo.endswith(".CNAECSV"):
            caminho_antigo = os.path.join(CAMINHO_PASTA, nome_arquivo)
            caminho_novo = os.path.join(CAMINHO_PASTA, nome_arquivo)
            os.rename(caminho_antigo, caminho_novo)
            file_name = caminho_novo
            bucket_name = 'abrasel-datalake'  # Nome do bucket correto
            object_name = 'raw/CNPJ/cnpj_cnae/'+nome_arquivo  # Caminho dentro do bucket
            upload_to_s3(file_name, bucket_name, object_name)
        else:
            print('Erro: Arquivo .CNAECSV não foi encontrado!!!')
            pass

    print('Arquivos CNAES enviados com sucesso!')

def baixar_simples(link_ultima_atualizacao):
    try:
        simples_url = link_ultima_atualizacao + '/simples.zip'
        print('simples_url: ',simples_url)
        print('\nBaixando SIMPLES . . .')

        response = requests.get(simples_url)
        if response.status_code == 200:
            print('\nDownload efetuado com sucesso!')
            zip_data = BytesIO(response.content)
            extrair_um_arquivo(zip_data)
        else:
            print('ERRO: Houve uma instabilidade no download.')
            baixar_simples(link_ultima_atualizacao)
            
    except:
        print('ERRO: Não foi possível baixar SIMPLES. Verifique a conexão com LINK {}'.format(simples_url))
        baixar_simples(link_ultima_atualizacao)
        
def enviar_simples_aws():
    diretorio = 'raw/CNPJ/cnpj_simples/'
    excluir_arquivos(diretorio)
    print('Enviando arquivos .SIMPLES para AWS . . .')
    for nome_arquivo in os.listdir(CAMINHO_PASTA):
        if ".SIMPLES" in nome_arquivo:
            caminho_antigo = os.path.join(CAMINHO_PASTA, nome_arquivo)
            caminho_novo = os.path.join(CAMINHO_PASTA, nome_arquivo)
            os.rename(caminho_antigo, caminho_novo)
            file_name = caminho_novo
            bucket_name = 'abrasel-datalake'  # Nome do bucket correto
            object_name = 'raw/CNPJ/cnpj_simples/'+nome_arquivo  # Caminho dentro do bucket
            upload_to_s3(file_name, bucket_name, object_name)

    print('Arquivos SIMPLES enviados com sucesso!')

def extrair_um_arquivo(zip_data):
    print('Extraindo arquivo...')
    try:
            with zipfile.ZipFile(zip_data) as zip_ref:
                zip_ref.extractall(CAMINHO_PASTA)
                print('ARQUIVO EXTRAIDO COM SUCESSO!')
                
    except:
        print('Erro ao tentar extrair arquivo!!!\nTentando novamente . . .')
        extrair_um_arquivo(zip_data)
    

    
def baixar_estabelecimentos(link_ultima_atualizacao,CONTADOR_ESTABELECIMENTOS):
    if (CONTADOR_ESTABELECIMENTOS < 10):
        try:
            zip_url = link_ultima_atualizacao+ '/Estabelecimentos{}.zip'.format(CONTADOR_ESTABELECIMENTOS)
            print('\nBaixando dados de ESTABELECIMENTOS arquivo nº {}'.format(CONTADOR_ESTABELECIMENTOS))
            print('aaa')
            response = requests.get(zip_url, timeout=(20, 240))
            print('oi')

            if response.status_code == 200:
                print('\nDownload efetuado com sucesso!')
                zip_data = BytesIO(response.content)
                extrair_estabelecimentos(zip_data, CONTADOR_ESTABELECIMENTOS)
                CONTADOR_ESTABELECIMENTOS = CONTADOR_ESTABELECIMENTOS + 1
                baixar_estabelecimentos(link_ultima_atualizacao,CONTADOR_ESTABELECIMENTOS)       
            else:
                print('ERRO: Houve uma instabilidade no download ESTABELECIMENTOS.')
                baixar_estabelecimentos(link_ultima_atualizacao,CONTADOR_ESTABELECIMENTOS)
                
        except Exception as e:
            print('ERRO: Não foi possível baixar EMPRESAS. Verifique a conexão com LINK {}.\nTentando download novamente.'.format(zip_url, CONTADOR_EMPRESAS))
            baixar_estabelecimentos(link_ultima_atualizacao,CONTADOR_ESTABELECIMENTOS)
            
    else:
        print("Todos os arquivos ESTABELECIMENTOS foram baixados e extraídos com sucesso.")
        CONTADOR_ESTABELECIMENTOS = 0
        
def extrair_estabelecimentos(zip_data,CONTADOR_ESTABELECIMENTOS):
    print('Extraindo arquivo...')
    try:
        with zipfile.ZipFile(zip_data) as zip_ref:
            zip_ref.extractall(CAMINHO_PASTA)
            print('Arquivo nº{} extraído com sucesso!'.format(CONTADOR_ESTABELECIMENTOS))
    except:
        print('Erro ao tentar extrair arquivo {}.\nTetanto extrair novamente. . .' .format(CONTADOR_ESTABELECIMENTOS))
        extrair_estabelecimentos(zip_data,CONTADOR_ESTABELECIMENTOS)
        
def enviar_estabelecimentos_aws():
    diretorio = 'raw/CNPJ/cnpj_estabele/'
    excluir_arquivos(diretorio)
    i = 0
    for nome_arquivo in os.listdir(CAMINHO_PASTA):
        if nome_arquivo.endswith(".ESTABELE"):
            caminho_antigo = os.path.join(CAMINHO_PASTA, nome_arquivo)
            novo_nome = "ESTABELECIMENTOS{}.ESTABELE".format(i)
            print('novo_nome: ',novo_nome)
            caminho_novo = os.path.join(CAMINHO_PASTA, novo_nome)
            os.rename(caminho_antigo, caminho_novo)
            file_name = caminho_novo
            bucket_name = 'abrasel-datalake'  # Nome do bucket correto
            object_name = 'raw/CNPJ/cnpj_estabele/'+novo_nome  # Caminho dentro do bucket
            print('object_name: ', object_name)
            upload_to_s3(file_name, bucket_name, object_name)
            i = i+1
    print('Arquivos ESTABELECIMENTOS enviados com sucesso!')


if __name__ == "__main__":
    # Obtém o link da última atualização
    link_ultima_atualizacao = obter_links_ultima_atualizacao(URL_RF)
    print(f"Link da última atualização: {link_ultima_atualizacao}")
    #baixar_cnaes(link_ultima_atualizacao)
    #enviar_cnaes_aws()
    #baixar_simples(link_ultima_atualizacao)
    #enviar_simples_aws()
    baixar_estabelecimentos(link_ultima_atualizacao,CONTADOR_ESTABELECIMENTOS)
    #enviar_estabelecimentos_aws()
    baixar_empresas(link_ultima_atualizacao,CONTADOR_EMPRESAS)
    #enviar_empresas_aws()

