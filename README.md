{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": [],
      "collapsed_sections": [
        "load-section"
      ],
      "toc_visible": true,
      "include_colab_link": true
    },
    "kernelspec": {
      "name": "python3",
      "display_name": "Python 3"
    },
    "language_info": {
      "name": "python"
    }
  },
  "cells": [
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "title"
      },
      "source": [
        "# Santander Dev Week 2023 — ETL com Python\n",
        "\n",
        "## Projeto adaptado para execução local\n",
        "\n",
        "Este notebook demonstra um fluxo completo de **ETL (Extract, Transform, Load)** sem depender da API original, que pode estar indisponível.\n",
        "\n",
        "### Objetivo\n",
        "- Extrair dados de clientes a partir de um arquivo CSV\n",
        "- Transformar os dados gerando mensagens personalizadas\n",
        "- Carregar os resultados em arquivos locais JSON e CSV\n",
        "\n",
        "Este formato mantém o desafio válido e adequado para portfólio."
      ]
    },
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "csv-structure"
      },
      "source": [
        "## Estrutura esperada do arquivo `SDW2023.csv`\n",
        "\n",
        "```csv\n",
        "UserID,name,account_number,card_number\n",
        "1,Gustavo,00001-1,1111\n",
        "2,Ana,00002-2,2222\n",
        "3,Carlos,00003-3,3333\n",
        "```"
      ]
    },
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "imports-title"
      },
      "source": [
        "## 1. Importação das bibliotecas"
      ]
    },
    {
      "cell_type": "code",
      "metadata": {
        "id": "imports-code"
      },
      "execution_count": null,
      "outputs": [],
      "source": [
        "import pandas as pd\n",
        "import json\n",
        "from pathlib import Path"
      ]
    },
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "extract-title"
      },
      "source": [
        "## 2. Extract\n",
        "\n",
        "Leitura do CSV local e preparação dos dados."
      ]
    },
    {
      "cell_type": "code",
      "metadata": {
        "id": "extract-functions"
      },
      "execution_count": null,
      "outputs": [],
      "source": [
        "def extract_data(file_path='SDW2023.csv'):\n",
        "    path = Path(file_path)\n",
        "    \n",
        "    if not path.exists():\n",
        "        raise FileNotFoundError(f'Arquivo não encontrado: {file_path}')\n",
        "    \n",
        "    df = pd.read_csv(path)\n",
        "    required_columns = {'UserID', 'name', 'account_number', 'card_number'}\n",
        "    missing_columns = required_columns - set(df.columns)\n",
        "    \n",
        "    if missing_columns:\n",
        "        raise ValueError(f'Colunas obrigatórias ausentes no CSV: {missing_columns}')\n",
        "    \n",
        "    users = df.to_dict(orient='records')\n",
        "    \n",
        "    for user in users:\n",
        "        user['news'] = []\n",
        "    \n",
        "    print(f'[EXTRACT] {len(users)} usuários carregados com sucesso.')\n",
        "    return users"
      ]
    },
    {
      "cell_type": "code",
      "metadata": {
        "id": "extract-run"
      },
      "execution_count": null,
      "outputs": [],
      "source": [
        "users = extract_data('SDW2023.csv')\n",
        "print(json.dumps(users, indent=2, ensure_ascii=False))"
      ]
    },
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "transform-title"
      },
      "source": [
        "## 3. Transform\n",
        "\n",
        "Geração das mensagens personalizadas para cada cliente."
      ]
    },
    {
      "cell_type": "code",
      "metadata": {
        "id": "transform-functions"
      },
      "execution_count": null,
      "outputs": [],
      "source": [
        "def generate_marketing_message(user):\n",
        "    name = user['name']\n",
        "    \n",
        "    return (\n",
        "        f'Olá {name}, investir com estratégia é uma forma inteligente de cuidar do seu futuro. '\n",
        "        f'Comece hoje a construir uma vida financeira mais segura e estável.'\n",
        "    )\n",
        "\n",
        "def transform_data(users):\n",
        "    for user in users:\n",
        "        message = generate_marketing_message(user)\n",
        "        user['news'].append({\n",
        "            'icon': 'https://digitalinnovationone.github.io/santander-dev-week-2023-api/icons/credit.svg',\n",
        "            'description': message\n",
        "        })\n",
        "    \n",
        "    print(f'[TRANSFORM] Mensagens geradas para {len(users)} usuários.')\n",
        "    return users"
      ]
    },
    {
      "cell_type": "code",
      "metadata": {
        "id": "transform-run"
      },
      "execution_count": null,
      "outputs": [],
      "source": [
        "users = transform_data(users)\n",
        "print(json.dumps(users, indent=2, ensure_ascii=False))"
      ]
    },
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "load-section"
      },
      "source": [
        "## 4. Load\n",
        "\n",
        "Salvando os dados transformados em arquivos JSON e CSV."
      ]
    },
    {
      "cell_type": "code",
      "metadata": {
        "id": "load-functions"
      },
      "execution_count": null,
      "outputs": [],
      "source": [
        "def load_to_json(users, output_file='users_output.json'):\n",
        "    with open(output_file, 'w', encoding='utf-8') as f:\n",
        "        json.dump(users, f, indent=2, ensure_ascii=False)\n",
        "    print(f'[LOAD] Arquivo JSON salvo com sucesso: {output_file}')\n",
        "\n",
        "def load_to_csv(users, output_file='SDW2023_output.csv'):\n",
        "    output_data = []\n",
        "    \n",
        "    for user in users:\n",
        "        output_data.append({\n",
        "            'UserID': user['UserID'],\n",
        "            'name': user['name'],\n",
        "            'account_number': user['account_number'],\n",
        "            'card_number': user['card_number'],\n",
        "            'news': user['news'][-1]['description'] if user['news'] else ''\n",
        "        })\n",
        "    \n",
        "    output_df = pd.DataFrame(output_data)\n",
        "    output_df.to_csv(output_file, index=False, encoding='utf-8')\n",
        "    print(f'[LOAD] Arquivo CSV salvo com sucesso: {output_file}')"
      ]
    },
    {
      "cell_type": "code",
      "metadata": {
        "id": "load-run"
      },
      "execution_count": null,
      "outputs": [],
      "source": [
        "load_to_json(users)\n",
        "load_to_csv(users)\n",
        "\n",
        "print('\\n[ETL] Processo concluído com sucesso!')"
      ]
    },
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "validation-title"
      },
      "source": [
        "## 5. Validação rápida dos resultados"
      ]
    },
    {
      "cell_type": "code",
      "metadata": {
        "id": "validation-run"
      },
      "execution_count": null,
      "outputs": [],
      "source": [
        "output_df = pd.read_csv('SDW2023_output.csv')\n",
        "output_df.head()"
      ]
    },
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "conclusion"
      },
      "source": [
        "## Conclusão\n",
        "\n",
        "Este projeto demonstra um fluxo ETL completo:\n",
        "\n",
        "- **Extract:** leitura dos dados a partir de um CSV\n",
        "- **Transform:** criação de mensagens personalizadas\n",
        "- **Load:** salvamento dos resultados em JSON e CSV\n",
        "\n",
        "Mesmo sem a API original, o desafio continua atendido e o projeto pode ser usado em portfólio."
      ]
    }
  ]
}
