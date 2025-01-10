# Guia de Configuração e Execução do Projeto

## Sumário

- [Visão Geral](#visão-geral)
- [Requisitos](#requisitos)
  - [Windows](#windows)
  - [Linux](#linux)
- [Instalação e Configuração](#instalação-e-configuração)
  - [Clonar o Repositório](#clonar-o-repositório)
  - [Criar Ambiente Virtual](#criar-ambiente-virtual)
  - [Instalar Dependências](#instalar-dependências)
  - [Configuração Adicional no Windows (Spark e Delta Lake)](#configuração-adicional-no-windows-spark-e-delta-lake)
- [Variáveis de Ambiente](#variáveis-de-ambiente)
- [Execução do Projeto](#execução-do-projeto)
  - [Ambiente de Desenvolvimento](#ambiente-de-desenvolvimento)
  - [Build para Produção](#build-para-produção)
- [Endpoints da API](#endpoints-da-api)
  - [Gerenciamento de Arquivos](#gerenciamento-de-arquivos)
  - [Operações de Transformação](#operações-de-transformação)
  - [Integração com Banco de Dados](#integração-com-banco-de-dados)

---

## Visão Geral
Este projeto é uma API de processamento de dados que permite realizar transformações em conjuntos de dados utilizando Spark e Delta Lake.

## Requisitos

### Windows
- Python 3.8 ou superior
- Java JDK 8 ou superior
- Apache Spark 3.5.0
- Hadoop `winutils.exe` (veja a [Configuração Adicional no Windows](#configuração-adicional-no-windows-spark-e-delta-lake))

### Linux
- Python 3.8 ou superior
- Java JDK 8 ou superior
- Apache Spark 3.5.0

---

## Instalação e Configuração

### Clonar o Repositório
```bash
# Clonar o repositório
$ git clone <URL_DO_REPOSITORIO>
$ cd <NOME_DO_PROJETO>
```

### Criar Ambiente Virtual
```bash
# Criar ambiente virtual
$ python -m venv venv
# Ativar o ambiente virtual
# No Windows
$ .\venv\Scripts\activate
# No Linux/Mac
$ source venv/bin/activate
```

### Instalar Dependências
```bash
# Instalar dependências
$ pip install -r requirements.txt
```

### Configuração Adicional no Windows (Spark e Delta Lake)

#### Baixar `winutils.exe`
1. Faça o download do arquivo `winutils.exe` correspondente à sua versão do Hadoop (testado com a versão 3.0.0):
   - [winutils - Hadoop binaries for Windows](https://github.com/steveloughran/winutils)

2. Crie o seguinte diretório para o Hadoop em sua máquina local:
   ```
   C:\hadoop\bin
   ```

3. Coloque o arquivo `winutils.exe` na pasta `C:\hadoop\bin`.

#### Configurar Variáveis de Ambiente
1. Abra o **Prompt de Comando** ou **PowerShell** com permissões de administrador.
2. Configure as variáveis de ambiente do Hadoop e do Spark:

   - No **Prompt de Comando**:
     ```cmd
     set HADOOP_HOME=C:\hadoop
     set PATH=%PATH%;C:\hadoop\bin
     ```

   - No **PowerShell**:
     ```powershell
     $env:HADOOP_HOME="C:\hadoop"
     $env:PATH="$env:PATH;C:\hadoop\bin"
     ```

3. Adicione as variáveis de ambiente permanentemente:
   - Pesquise por "Variáveis de Ambiente" no menu iniciar e abra.
   - No painel "Variáveis de Sistema", adicione:
     - **Nome da variável**: `HADOOP_HOME`
       **Valor da variável**: `C:\hadoop`
     - Edite a variável `PATH` e adicione: `C:\hadoop\bin`.

---

## Variáveis de Ambiente
Crie um arquivo `.env` na raiz do projeto com o seguinte conteúdo:

```env
DB_USER=<usuario_do_banco>
DB_PASSWORD=<senha_do_banco>
DB_HOST=<host_do_banco>
DB_PORT=<porta_do_banco>
DB_NAME=<nome_do_banco>
```

---

## Execução do Projeto

### Ambiente de Desenvolvimento
```bash
# Ativar o ambiente virtual (caso ainda não esteja ativo)
# No Windows
$ .\venv\Scripts\activate
# No Linux/Mac
$ source venv/bin/activate

# Executar o servidor Flask
$ python app.py
```

A aplicação estará disponível em `http://127.0.0.1:5000`.

## Endpoints da API

### Gerenciamento de Arquivos

#### Upload de Arquivo
**`POST /upload/<project_id>`**
- Realiza o upload de um arquivo para o projeto especificado.

#### Excluir Arquivo
**`DELETE /file/delete`**
- Remove um arquivo previamente enviado.

### Operações de Transformação

#### Substituir Valores
**`POST /flow/<flow_id>/replace_value`**
- Substitui valores em uma coluna do fluxo Delta.

#### Transpor Dados
**`POST /flow/<flow_id>/transpor`**
- Transpõe os dados do fluxo Delta.

#### Renomear Colunas
**`POST /flow/<flow_id>/rename_column`**
- Renomeia colunas no fluxo Delta.

#### Calcular Nova Coluna
**`POST /flow/<flow_id>/calcular_nova_coluna`**
- Adiciona uma nova coluna calculada ao fluxo Delta.

#### Sumarizar Dados
**`POST /flow/<flow_id>/sumarizar`**
- Realiza agregações e sumarizações no fluxo Delta.

#### Calcular Média Ponderada
**`POST /flow/<flow_id>/calcular_media_ponderada`**
- Calcula a média ponderada de colunas no fluxo Delta.

### Integração com Banco de Dados

#### Carregar Dados do Banco de Dados
**`POST /flow/<flow_id>/load_from_db`**
- Carrega dados de um banco para o fluxo Delta.
- Requer `db_type`, `table_name` ou `query`.

---

