# Desafio Técnico Multiplan

## 1. Objetivos
Este projeto tem por objetivo responder as seguintes questões:

- formatar uma saída do log em json contendo a lista de request apresentada no log, cada objeto dentro da lista deve conter as propriedades de uma entrada no log como remote_host, date, request, status_code, response_time, reffer, user_agent.
- encontrar os 10 maiores tempos de resposta com sucesso do servidor na chamada GET /maunal/ com a origem do tráfego (reffer) igual a "http://localhost/svnview?repos=devel&rev=latest&root=SVNview/tmpl&list_revs=1"
- formatar uma saída em arquivo físico do access.log com a data em formato UNIX timestamp %Y-%m-%d %H:%M:%S e o IP convertido em um hash MD5
- formatar uma saída em arquivo físico agrupando a soma total de requests por dia do ano
- formatar uma saída em arquivo físico com endereços de IP únicos, um IP por linha, contidos no log com a última data de request realizado pelo remote IP

Dessa forma, para cada questão será gerado um arquivo correspondente, seguindo a ordem das questões acima. Os arquivos se encontram no repositório na pasta output, porém para gerar localmente é só executar o projeto, mais a frente uma explicação mais detalhada de como o fazer. Segue a ordem dos arquivos em relação as questões:

* test-access-001-1.json
* top10_requests_by_response_tim.csv
* test-access-hashed.csv
* total-requests-by-day.csv
* unique-ips-by-last-request


## 2. Organização do projeto
```
multiplan-challenge/
│── app/                   # Scripts com toda a lógica do projeto
| │── parsing.py           # Camada de parsing e casting
| │── transformation.py    # Camada de transformação de dados
| │── io_utils.py          # Camada pra gerir o I/O
| │── main.py              # Orquestração geral do pipeline
│── data/                  # Arquivos de input
│── docs/                  # Documentações do projeto
│── output/                # Arquivos de output gerados pelos scripts
│── tests/                 # Testes Unitários
│ │── test_parsing.py
│ │── test_transformation.py
│ │── test_io_utils.py
│── requirements.txt       # Dependências do projeto
│── .gitignore             # Arquivos a serem ignorados
│── .python-version        # Versão do Python utilizada
```

## 3. Como Executar o Projeto

### 3.1 Clonando o projeto

```bash
git clone https://github.com/gabrieldinizsm/multiplan-challenge.git
```

### 3.2 Pré-requisitos

- Python 3.12.1 (versão recomendada)
- Recomendado: virtual environment

### 3.3 Setup do ambiente

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
.venv\Scripts\activate     # Windows
pip install -r requirements.txt
```

### 3.4 Executando o pipeline

```bash
python main.py
```

### 3.5 Output

Os seguintes arquivos irão ser gerados no diretório output:

* test-access-001-1.json
* top10_requests_by_response_tim.csv
* test-access-hashed.csv
* total-requests-by-day.csv
* unique-ips-by-last-request

## 4. Configuração

DATA_DIR e OUTPUT_DIR – Diretórios de input (data) e output (output) respectivamente (no script main.py)

TARGET_REFERRER – Referrer a ser filtrada para resolver a questão dois

## 5. Testes Unitários

Módulo contendo os testes unitários das camadas do projeto

## 5.1 Organização dos testes
```
│── test_parsing.py           # Testes para camada parsing
│── test_transformation.py    # Testes para camada transformation
│── test_io_utils.py          # Testes para camada io_utils
```

## 5.2 Setup para rodar os testes

Dentro do diretório root do projeto executar:

```bash
PYTHONPATH=. pytest #Linux/MacOS
```

ou

```bash
$env:PYTHONPATH="."; pytest #Windows
```

## 5.3 Executando os testes

Ainda no diretório root executar:

```bash
pytest
```

Para executar somente os testes de um arquivo, como por exemplo apenas os testes do test_parsing.py:

```bash
pytest tests/test_parsing.py
```
