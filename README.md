# Projeto ETL Funcional: Processamento de Pedidos e Receitas

> **Disciplina:** Programação Funcional / Engenharia de Dados  
> **Linguagem:** F# (.NET)  

## Objetivo do Projeto
Este projeto implementa um pipeline ETL (Extract, Transform, Load) utilizando o paradigma da programação funcional em F#. O objetivo é processar dados de pedidos e itens de um sistema de gestão, extraídos de fontes externas, aplicando regras de negócio para calcular receitas e impostos agregados, e por fim, disponibilizar essas informações para alimentar dashboards de Business Intelligence (BI).

O projeto prioriza a **imutabilidade**, o uso de **funções puras** e **funções de ordem superior** (`map`, `filter`, `fold`), isolando estritamente os efeitos colaterais (operações de I/O, chamadas HTTP e acessos a banco de dados).

## Arquitetura e Modularização
O código foi estruturado em um projeto .NET e dividido para garantir a separação clara entre funções puras (lógica de negócio) e impuras (acesso a dados):

* **`Types.fs`**: Contém a definição das estruturas de dados usando `Records` (ex: `Order`, `OrderItem`, `AggregatedOrder`).
* **`DataAccess.fs` (Impuro)**: Gerencia os *side effects*. Inclui funções para buscar os arquivos via HTTP, leitura de arquivos CSV e persistência no banco de dados relacional. Contém as *Helper Functions* responsáveis por mapear os dados brutos (strings) para as listas de `Records`.
* **`Transform.fs` (Puro)**: O núcleo lógico do pipeline. Totalmente puro e coberto por testes. Aqui ocorrem:
  * O *Inner Join* entre a lista de `Order` e `OrderItem` diretamente em memória usando F#.
  * O cálculo de `total_amount` e `total_taxes` utilizando `map` e `fold`.
  * A parametrização e filtragem por `status` e `origin` utilizando `filter`.
  * A agregação secundária (média de receita e impostos agrupados por mês e ano).
* **`Program.fs` (Impuro)**: O ponto de entrada da aplicação, responsável por orquestrar o fluxo do ETL compondo as funções dos módulos acima.

## Dicionário de Dados e Transformações

### Extração (Input)
Os dados são lidos a partir de arquivos CSV (ou endpoints HTTP):
1. **`Order`**: `id` (PK), `client_id`, `order_date` (ISO 8601), `status` (*pending, complete, cancelled*), `origin` (*P - physical, O - online*).
2. **`OrderItem`**: `order_id` (FK), `product_id` (PK), `quantity`, `price`, `tax` (percentual).

### Transformação & Carga (Output)
O sistema gera duas saídas principais:
1. **Relatório Parametrizado (CSV/BD):** * Campos: `order_id`, `total_amount` (Σ preço * quantidade), `total_taxes` (Σ receita do item * imposto percentual).
   * Suporta filtragem customizada por status e origem (ex: apenas `complete` e `online`).
2. **Relatório Agregado Mensal:**
   * Média de receita e impostos consolidados por Mês e Ano.

## Estrutura do Projeto
```text
📦 Projeto_ETL
 ┣ 📂 src
 ┃ ┣ 📜 Types.fs
 ┃ ┣ 📜 DataAccess.fs
 ┃ ┣ 📜 Transform.fs
 ┃ ┣ 📜 Program.fs
 ┃ ┗ 📜 NomeDoSeuProjeto.fsproj
 ┣ 📂 tests
 ┃ ┣ 📜 TransformTests.fs
 ┃ ┗ 📜 NomeDoSeuProjeto.Tests.fsproj
 ┣ 📂 docs
 ┃ ┗ 📜 Relatorio_Projeto.pdf
 ┣ 📜 .gitignore
 ┗ 📜 README.md