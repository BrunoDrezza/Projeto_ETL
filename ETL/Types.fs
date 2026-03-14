namespace ETL

/// <summary>
/// Módulo de Domínio: Order (Pedido)
/// Isola todas as regras, tipos e definições referentes ao cabeçalho de uma ordem.
/// No F#, uma Single-Case Discriminated Union (União Discriminada de Caso Único) 
/// é uma maneira simples e leve de criar um tipo novo e distinto que encapsula 
/// um valor base (como um 'int' ou 'string'). 
///
/// Essa técnica é usada principalmente para evitar a "Obsessão Primitiva" 
/// (Primitive Obsession), dando aos tipos primitivos nomes semanticamente 
/// relevantes. Isso melhora drasticamente a segurança de tipos (Type Safety) 
/// e torna o código autodocumentado.
/// </summary>


module Order =

    // ========================================================================
    // 1. SINGLE-CASE DISCRIMINATED UNIONS
    // ========================================================================
    // Usados em sala para criar tipios que guardam valores de inteiros 
    // Por que não usar apenas 'int' ou 'DateTime'? 
    // Se usássemos 'int' para Id e 'int' para ClientId, um desenvolvedor poderia 
    // acidentalmente passar o ID do Cliente na função de buscar Pedido, e o compilador aceitaria.
    // Ao "embrulhar" os tipos primitivos nessas caixas, garantimos Segurança de Tipos (Type Safety) Absoluta.
    
    /// Identificador único da Ordem.
    type Id = Id of int

    /// Identificador do Cliente que originou a Ordem.
    type ClientId = ClientId of int

    /// Data exata em que a ordem foi registrada (Fuso horário normalizado).
    type OrderDate = OrderDate of System.DateTime
    
    // ========================================================================
    // 2. DISCRIMINATED UNIONS (Máquinas de Estado Finita)
    // ========================================================================
    // Substituem o uso de "strings soltas". 
    // Isso garante que é IMPOSSÍVEL existir um status "pendind" (escrito errado).
    // O sistema só aceita os estados explicitamente declarados aqui.

    /// Estado atual do processamento da Ordem.
    type Status =
        | Pending
        | Complete
        | Cancelled
        
    /// Canal de origem de onde a Ordem foi emitida.
    type Origin =
        | Presencial
        | Online

    // ========================================================================
    // 3. RECORD TYPE (A Entidade Final)
    // ========================================================================
    // Records são estruturas de dados estritamente Imutáveis.
    // Uma vez que uma Ordem é criada no parser, ela nunca mais pode ser alterada.
    // Ela serve como a "Verdade Única" para o resto do pipeline de ETL.

    /// Representação completa e validada do cabeçalho de uma Ordem.
    type Order = {
        Id: Id
        Origin: Origin
        Status: Status
        OrderDate: OrderDate
        ClientId: ClientId
    }

/// <summary>
/// Módulo de Domínio: OrderItem (Itens do Pedido)
/// Isola as definições matemáticas e identificadores das linhas granulares das ordens.
/// </summary>

module OrderItem = 

    // O encapsulamento aqui é crucial para operações financeiras.
    // Impede que alguém some acidentalmente a Quantidade (int) com o OrderId (int).

    /// Chave estrangeira que vincula este item ao cabeçalho da Ordem.
    type OrderId = OrderId of int

    /// Identificador único do ativo/produto transacionado.
    type ProductId = ProductId of int

    /// Volume financeiro ou número de cotas transacionadas.
    type Quantity = Quantity of int

    /// Preço unitário do ativo no momento da transação (Alta precisão requerida).
    type Price = Price of float

    /// Custos operacionais, taxas de corretagem ou impostos retidos.
    type Tax = Tax of float

    /// Representação completa e validada de uma linha transacional.
    type OrderItem = {
        OrderId: OrderId
        ProductId: ProductId
        Quantity: Quantity
        Price: Price
        Tax: Tax
    }