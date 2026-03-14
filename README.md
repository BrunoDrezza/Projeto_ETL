# Projeto ETL Funcional: Processamento de Pedidos e Receitas

> **Disciplina:** Programação Funcional
> **Linguagem:** F# (.NET)  

## Objetivo do Projeto
Este projeto implementa um pipeline ETL (Extract, Transform, Load) utilizando o paradigma da programação funcional em F#. O objetivo é processar dados de pedidos e itens de um sistema de gestão, extraídos de fontes externas via HTTP, aplicando regras de negócio para calcular receitas e impostos agregados, e por fim, disponibilizar essas informações em um banco de dados relacional para alimentar dashboards de Business Intelligence (BI).

O projeto atende a rigorosos critérios de **imutabilidade**, uso de **funções puras** e **funções de ordem superior** (`map`, `filter`, `fold`), isolando estritamente os efeitos colaterais em um projeto separado.

## Arquitetura e Modularização
O ecossistema da solução foi dividido em três projetos .NET distintos para garantir a separação física entre o domínio puro, as operações de I/O e a validação:

* **`ETL` (Projeto Puro)**: O núcleo lógico do pipeline. Totalmente puro, determinístico e livre de efeitos colaterais. Responsável por:
  * Definir as estruturas de dados imutáveis (`Records`).
  * Realizar o *Inner Join* entre a lista de pedidos e itens em memória.
  * Executar os cálculos de `total_amount` e `total_taxes` utilizando `map` e `fold`.
  * Aplicar filtros parametrizados por `status` e `origin`.
  * Calcular a agregação secundária (média de receita e impostos agrupados por mês e ano).
* **`Main` (Projeto Impuro)**: O orquestrador e gerenciador de *side effects*. Responsável por:
  * Extração: Leitura dos dados de entrada de arquivos estáticos na internet via HTTP.
  * *Helper Functions*: Conversão dos dados textuais brutos para os `Records` do projeto `ETL`.
  * Carga: Conexão e persistência dos dados finais no banco de dados relacional.
  * Ponto de entrada (`Program.fs`) que interliga a extração, a transformação (chamando o projeto `ETL`) e a carga.
* **`ETL.Tests` (Projeto de Testes)**: Suíte de testes automatizados dedicados exclusivamente a validar o comportamento das funções puras contidas no projeto `ETL`, garantindo a integridade da lógica de negócio.

## Dicionário de Dados e Transformações

### Extração (Input)
Os dados são lidos a partir de arquivos CSV expostos via HTTP:
1. **`Order`**: `id` (PK), `client_id`, `order_date` (ISO 8601), `status` (*pending, complete, cancelled*), `origin` (*P - physical, O - online*).
2. **`OrderItem`**: `order_id` (FK), `product_id` (PK), `quantity`, `price`, `tax` (percentual).

### Transformação & Carga (Output)
O sistema gera duas saídas principais:
1. **Relatório Parametrizado:**
   * Campos: `order_id`, `total_amount` (Σ preço * quantidade), `total_taxes` (Σ receita do item * imposto percentual).
   * Suporta filtragem customizada por status e origem (ex: apenas `complete` e `online`).
2. **Relatório Agregado Mensal:**
   * Média de receita e impostos consolidados por mês e ano.

## Estrutura de Diretórios
```text
📦 Projeto_ETL
 ┣ 📂 ETL
 ┃ ┣ 📜 Types.fs
 ┃ ┣ 📜 Transform.fs
 ┃ ┣ 📜 Parser.fs
 ┃ ┗ 📜 ETL.fsproj
 ┣ 📂 Main
 ┃ ┣ 📜 Main.fs
 ┃ ┗ 📜 Main.fsproj
 ┣ 📂 ETL.Tests
 ┃ ┣ 📜 TransformTests.fs
 ┃ ┣ 📜 ParserTests.fs
 ┃ ┗ 📜 ETL.Tests.fsproj
 ┣ 📂 docs
 ┃ ┗ 📜 Relatorio_Projeto.pdf
 ┣ 📜 .gitignore
 ┗ 📜 README.md