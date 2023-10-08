#%%
import os
import re
import sys
import wget
import time
import psycopg2
import zipfile
import urllib.request

import bs4 as bs
import pandas as pd

from datetime   import date
from dotenv     import load_dotenv, find_dotenv
from pathlib    import Path
from sqlalchemy import create_engine

load_dotenv(find_dotenv())

#%%
# Função para obter variáveis de ambiente
def get_environment_variable(env_var_name):
    """
    Obtém o valor de uma variável de ambiente.

    Args:
        env_var_name (str): Nome da variável de ambiente.

    Returns:
        str: Valor da variável de ambiente.
    ref: https://dev.to/jakewitcher/using-env-files-for-environment-variables-in-python-applications-55a1
    """
    return os.getenv(env_var_name)

# URL dos dados
dados_url = 'http://200.152.38.155/CNPJ/'

try:
    # Ler caminhos dos diretórios a partir do arquivo de ambiente
    output_files_path = get_environment_variable('OUTPUT_FILES_PATH')
    extracted_files_path = get_environment_variable('EXTRACTED_FILES_PATH')

    print('Diretórios definidos: \n' +
          'output_files_path: ' + str(output_files_path) + '\n' +
          'extracted_files_path: ' + str(extracted_files_path))
except Exception as e:
    print('Erro na definição dos diretórios, verifique o arquivo ".env" ou o local informado do seu arquivo de configuração.')
    print(e)

# Fazer o download do conteúdo da página
raw_html = urllib.request.urlopen(dados_url)
raw_html = raw_html.read()

# Formatar página e converter em string
page_items = bs.BeautifulSoup(raw_html, 'lxml')
html_str = str(page_items)

# Obter arquivos
zip_file_links = []
text_to_find = '.zip'

for match in re.finditer(text_to_find, html_str):
    start_index = match.start() - 40
    end_index = match.end()
    location_index = html_str[start_index:end_index].find('href=') + 6
    zip_file_links.append(html_str[start_index + location_index:end_index])

# Correção do nome dos arquivos devido à mudança na estrutura do HTML da página - 31/07/22
cleaned_zip_file_links = [link for link in zip_file_links if not link.find('.zip">') > -1]

try:
    del zip_file_links
except Exception as e:
    print(e)

zip_file_links = cleaned_zip_file_links

print('Arquivos que serão baixados:')
for index, file_link in enumerate(zip_file_links):
    print(f'{index + 1} - {file_link}')

# Função para acompanhar o progresso do download
def download_progress(current, total, width=80):
    """
    Exibe o progresso do download na barra de progresso.

    Args:
        current (int): Bytes já baixados.
        total (int): Tamanho total do arquivo em bytes.
        width (int): Largura da barra de progresso.
    """
    progress_message = f"Downloading: {current / total * 100:.2f}% [{current} / {total}] bytes - "
    sys.stdout.write("\r" + progress_message)
    sys.stdout.flush()

#%%
# Download dos arquivos
for index, link in enumerate(zip_file_links):
    print('Baixando arquivo:')
    print(f'{index + 1} - {link}')
    url = dados_url + link
    output_path = output_files_path

    # Verifique se o nome do arquivo contém um ponto antes de dividir
    if '.' in link:
        filename, ext = link.rsplit('.', 1)
    else:
        filename = link

    wget.download(url, out=output_path, bar=download_progress)

#%%
# Diretório raiz (pasta de trabalho) onde você deseja excluir os arquivos .tmp
root_directory = os.getcwd()  # Obtém o diretório de trabalho atual

# Extensão dos arquivos que você deseja excluir (no caso, .tmp)
file_extension = '.tmp'

# Percorra todos os arquivos no diretório raiz
for filename in os.listdir(root_directory):
    # Verifique se o arquivo possui a extensão .tmp
    if filename.endswith(file_extension):
        file_path = os.path.join(root_directory, filename)
        try:
            # Remova o arquivo
            os.remove(file_path)
            print(f'Arquivo temporário excluído: {file_path}')
        except Exception as e:
            print(f'Erro ao excluir arquivo temporário: {file_path}')
            print(e)

#%%
# Extracting files:
for index, file_name in enumerate(os.listdir(output_files_path)):
    try:
        print('Descompactando arquivo:')
        print(f'{index + 1} - {file_name}')
        with zipfile.ZipFile(os.path.join(output_files_path, file_name), 'r') as zip_ref:
            # Obtenha o nome do arquivo sem a extensão .zip
            base_name = os.path.splitext(file_name)[0]
            
            # Crie o diretório de destino se não existir
            output_dir = os.path.join(extracted_files_path, base_name)
            os.makedirs(output_dir, exist_ok=True)
            
            # Extraia todos os arquivos do arquivo .zip para o diretório de destino
            zip_ref.extractall(output_dir)
            
            # Renomeie os arquivos extraídos para remover caracteres indesejados
            for extracted_file_name in os.listdir(output_dir):
                extracted_file_path = os.path.join(output_dir, extracted_file_name)
                new_file_name = f'{base_name}.csv'
                os.rename(extracted_file_path, os.path.join(output_dir, new_file_name))
            
        print(f'Arquivos extraídos e renomeados em: {output_dir}')
    except Exception as e:
        print(e)

#%%
# Lista os arquivos no diretório
import os

diretorio = r'D:\jupyter\ScoreEase\scoreease-etl-receita-federal\data\EXTRACTED_FILES'

# Use a função listdir para listar todos os arquivos e pastas no diretório
conteudo = os.listdir(diretorio)

# Filtrar apenas as pastas
pastas = [item for item in conteudo if os.path.isdir(os.path.join(diretorio, item))]

# Imprimir a lista de pastas
for pasta in pastas:
    print(pasta)

# %%
# Cria as 3 pastas no diretório
import os

diretorio = r'D:\jupyter\ScoreEase\scoreease-etl-receita-federal\data\EXTRACTED_FILES'
nomes_pastas = ['Empresas', 'Estabelecimentos', 'Socios']

for nome_pasta in nomes_pastas:
    caminho_pasta = os.path.join(diretorio, nome_pasta)

    # Verifique se a pasta já existe antes de criar
    if not os.path.exists(caminho_pasta):
        os.makedirs(caminho_pasta)
        print(f'A pasta {nome_pasta} foi criada com sucesso em {diretorio}.')
    else:
        print(f'A pasta {nome_pasta} já existe em {diretorio}.')


# %%
# Movendo todos os arquivos Empresas0.csv, Empresas1.csv etc para a pasta: Empresas
import os
import shutil

diretorio_base = r'D:\jupyter\ScoreEase\scoreease-etl-receita-federal\data\EXTRACTED_FILES'
pasta_destino = os.path.join(diretorio_base, 'Empresas')

# Loop de 0 a 9 para mover os arquivos de cada subdiretório
for i in range(10):
    pasta_origem = os.path.join(diretorio_base, f'Empresas{i}')
    
    # Verifique se a pasta de origem existe
    if os.path.exists(pasta_origem):
        # Liste todos os arquivos na pasta de origem
        arquivos = os.listdir(pasta_origem)
        
        # Mova os arquivos para a pasta de destino
        for arquivo in arquivos:
            caminho_origem = os.path.join(pasta_origem, arquivo)
            caminho_destino = os.path.join(pasta_destino, arquivo)
            shutil.move(caminho_origem, caminho_destino)
        
        # Remova a pasta de origem vazia
        os.rmdir(pasta_origem)

print(f'Arquivos movidos para {pasta_destino}.')

# %%
# O mesmo que acima, mas agora para Estabelecimentos
import os
import shutil

diretorio_base = r'D:\jupyter\ScoreEase\scoreease-etl-receita-federal\data\EXTRACTED_FILES'
pasta_destino = os.path.join(diretorio_base, 'Estabelecimentos')

# Loop de 0 a 9 para mover os arquivos de cada subdiretório
for i in range(10):
    pasta_origem = os.path.join(diretorio_base, f'Estabelecimentos{i}')
    
    # Verifique se a pasta de origem existe
    if os.path.exists(pasta_origem):
        # Liste todos os arquivos na pasta de origem
        arquivos = os.listdir(pasta_origem)
        
        # Mova os arquivos para a pasta de destino
        for arquivo in arquivos:
            caminho_origem = os.path.join(pasta_origem, arquivo)
            caminho_destino = os.path.join(pasta_destino, arquivo)
            shutil.move(caminho_origem, caminho_destino)
        
        # Remova a pasta de origem vazia
        os.rmdir(pasta_origem)

print(f'Arquivos movidos para {pasta_destino}.')


# %%
# O mesmo, mas agora para socios
import os
import shutil

diretorio_base = r'D:\jupyter\ScoreEase\scoreease-etl-receita-federal\data\EXTRACTED_FILES'
pasta_destino = os.path.join(diretorio_base, 'Socios')

# Loop de 0 a 9 para mover os arquivos de cada subdiretório
for i in range(10):
    pasta_origem = os.path.join(diretorio_base, f'Socios{i}')
    
    # Verifique se a pasta de origem existe
    if os.path.exists(pasta_origem):
        # Liste todos os arquivos na pasta de origem
        arquivos = os.listdir(pasta_origem)
        
        # Mova os arquivos para a pasta de destino
        for arquivo in arquivos:
            caminho_origem = os.path.join(pasta_origem, arquivo)
            caminho_destino = os.path.join(pasta_destino, arquivo)
            shutil.move(caminho_origem, caminho_destino)
        
        # Remova a pasta de origem vazia
        os.rmdir(pasta_origem)

print(f'Arquivos movidos para {pasta_destino}.')
#%%
# LER E INSERIR DADOS
insert_start = time.time()

# Files:
Items = [name for name in os.listdir(extracted_files_path) if name.endswith('')]

# Separar arquivos:
arquivos_empresa = []
arquivos_estabelecimento = []
arquivos_socios = []
arquivos_simples = []
arquivos_cnae = []
arquivos_moti = []
arquivos_munic = []
arquivos_natju = []
arquivos_pais = []
arquivos_quals = []
for i in range(len(Items)):
    if 'EMPRE' in Items[i]:
        arquivos_empresa.append(Items[i])
    elif 'ESTABELE' in Items[i]:
        arquivos_estabelecimento.append(Items[i])
    elif 'SOCIO' in Items[i]:
        arquivos_socios.append(Items[i])
    elif 'SIMPLES' in Items[i]:
        arquivos_simples.append(Items[i])
    elif 'CNAE' in Items[i]:
        arquivos_cnae.append(Items[i])
    elif 'MOTI' in Items[i]:
        arquivos_moti.append(Items[i])
    elif 'MUNIC' in Items[i]:
        arquivos_munic.append(Items[i])
    elif 'NATJU' in Items[i]:
        arquivos_natju.append(Items[i])
    elif 'PAIS' in Items[i]:
        arquivos_pais.append(Items[i])
    elif 'QUALS' in Items[i]:
        arquivos_quals.append(Items[i])
    else:
        pass

# Conectar no banco de dados:
# Dados da conexão com o BD
user = get_environment_variable('PG_USER')
passw = get_environment_variable('PG_PASSWORD')
host = get_environment_variable('PG_HOST')
port = get_environment_variable('PG_PORT')
database = get_environment_variable('PG_NAME')

# Conectar:
engine = create_engine(f'postgresql://{user}:{passw}@{host}:{port}/{database}')
conn = psycopg2.connect(f'dbname={database} user={user} host={host} password={passw}')
cur = conn.cursor()

# Arquivos de empresa:
empresa_insert_start = time.time()
print("""
#######################
## Arquivos de EMPRESA:
#######################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "empresa";')
conn.commit()

for e in range(0, len(arquivos_empresa)):
    print('Trabalhando no arquivo: '+arquivos_empresa[e]+' [...]')
    try:
        del empresa
    except:
        pass

    empresa = pd.DataFrame(columns=[0, 1, 2, 3, 4, 5, 6])
    empresa_dtypes = {0: 'object', 1: 'object', 2: 'object', 3: 'object', 4: 'object', 5: 'object', 6: 'object'}
    extracted_file_path = Path(f'{extracted_files_path}/{arquivos_empresa[e]}')

    empresa = pd.read_csv(filepath_or_buffer=extracted_file_path,
                          sep=';',
                          nrows=100,
                          skiprows=0,
                          header=None,
                          dtype=empresa_dtypes)

    # Tratamento do arquivo antes de inserir na base:
    empresa = empresa.reset_index()
    del empresa['index']

    # Renomear colunas
    empresa.columns = ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_federativo_responsavel']

    # Replace "," por "."
    empresa['capital_social'] = empresa['capital_social'].apply(lambda x: x.replace(',', '.'))
    empresa['capital_social'] = empresa['capital_social'].astype(float)

    # Gravar dados no banco:
    # Empresa
    empresa.to_sql(name='empresa', con=engine, if_exists='append', index=False)
    print('Arquivo ' + arquivos_empresa[e] + ' inserido com sucesso no banco de dados!')

try:
    del empresa
except:
    pass
print('Arquivos de empresa finalizados!')
empresa_insert_end = time.time()
empresa_Tempo_insert = round((empresa_insert_end - empresa_insert_start))
print('Tempo de execução do processo de empresa (em segundos): ' + str(empresa_Tempo_insert))