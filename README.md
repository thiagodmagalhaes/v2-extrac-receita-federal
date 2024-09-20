# Automação de Download e Envio de Dados da Receita Federal para AWS S3

## Sobre o Projeto

Este projeto foi desenvolvido para automatizar o processo de download, extração e envio de arquivos de dados abertos da Receita Federal (CNPJ) para um bucket no Amazon S3. O sistema faz o download dos arquivos mais recentes da Receita Federal, extrai os dados necessários e os envia para um bucket S3, de forma organizada e categorizada, garantindo a atualização contínua dos dados de empresas, estabelecimentos, CNAEs e informações do Simples Nacional.

## Funcionalidades

- **Download de Arquivos da Receita Federal**: O sistema realiza o download dos dados de empresas, estabelecimentos, CNAEs e Simples Nacional a partir do site da Receita Federal.
- **Extração de Arquivos ZIP**: Após o download, os arquivos são extraídos no diretório local do projeto.
- **Renomeação e Organização de Arquivos**: O sistema renomeia os arquivos de forma padronizada para facilitar a organização e envio para o S3.
- **Upload para AWS S3**: Os arquivos são enviados para um bucket específico no S3, com diretórios apropriados para cada tipo de dado.
- **Exclusão de Arquivos Existentes no S3**: Antes de enviar novos arquivos, o sistema exclui os arquivos antigos no S3, garantindo que apenas as últimas atualizações sejam mantidas.

## Como Executar o Projeto

### Pré-requisitos

- Python instalado
- Bibliotecas Python necessárias: `requests`, `beautifulsoup4`, `boto3`, `dotenv`
- Credenciais da AWS configuradas no `.env` ou diretamente no código

### Instruções de Execução

1. **Clonar o Repositório**:

  - ``git clone https://github.com/seu_usuario/seu_repositorio.git cd seu_repositorio``
  
2. **Instalar Dependências**:
No diretório do projeto, execute:

- ``pip install -r requirements.txt``

3. **Configurar Variáveis de Ambiente**:
Crie um arquivo `.env` com as credenciais da AWS:

- ``AWS_ACCESS_KEY_ID=SEU_ACCESS_KEY AWS_SECRET_ACCESS_KEY=SEU_SECRET_KEY``

4. **Executar o Script**:
Após configurar as credenciais, execute o script principal:

-``python nome_do_script.py``


## Tecnologias Utilizadas

- **Python**: Linguagem principal utilizada para automação.
- **Requests**: Utilizada para fazer requisições HTTP e baixar arquivos.
- **BeautifulSoup**: Usada para fazer scraping dos links no site da Receita Federal.
- **Boto3**: Biblioteca da AWS para interação com o S3, utilizada para upload e manipulação de arquivos no bucket.
- **Dotenv**: Utilizada para carregar as variáveis de ambiente, como credenciais da AWS.

Este projeto facilita a automatização do processo de obtenção de dados do CNPJ e o armazenamento seguro desses dados na AWS, tornando o processo mais eficiente e integrado.
