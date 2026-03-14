namespace ETL

module ReportEngine =
    
    // Importando os tipos
    module O = ETL.Order
    module OI = ETL.OrderItem

    /// <summary>
    /// Função 100% Pura. Recebe os parâmetros do gestor e as duas listas de banco de dados,
    /// e retorna um dicionário (Map) com o OrderId e os totais calculados.
    /// </summary>
    let calcularRelatorio (targetStatus: O.Status) (targetOrigin: O.Origin) (ordens: O.Order list) (itens: OI.OrderItem list) =
        
        // ========================================================================
        // ETAPA 1: O FILTRO (Filter)
        // ========================================================================
        // Queremos apenas os IDs das ordens que batem com o parâmetro do gestor.
        let idsValidos = 
            ordens
            // filter: Mantém apenas as ordens com o Status e Origem corretos
            |> List.filter (fun ord -> ord.Status = targetStatus && ord.Origin = targetOrigin)
            // map: Transforma a lista de 'Orders' em uma lista de 'Order.Id'
            |> List.map (fun ord -> ord.Id)
            // Rigor de Performance: Transforma a lista num Set (Conjunto) para busca ultra-rápida.
            |> Set.ofList 

        // ========================================================================
        // ETAPA 2: O MAPEAMENTO (Map)
        // ========================================================================
        let itensComReceita =
            itens
            // filter: Mantém APENAS os itens cujo OrderId existe no nosso Set de 'idsValidos'
            // O F# vai reclamar se você tentar comparar OI.OrderId com O.Id direto.
            // Precisamos desembrulhar os dois! Vamos extrair o 'int' para comparar.
            |> List.filter (fun item -> 
                let (OI.OrderId itemIdInt) = item.OrderId
                // Assumindo que O.Id e OI.OrderId guardam a mesma numeração inteira
                Set.exists (fun (O.Id ordIdInt) -> ordIdInt = itemIdInt) idsValidos
            )
            // map: Pega cada OrderItem puro e calcula a matemática financeira
            |> List.map (fun item -> 
                // "Desembrulhando" a Obsessão Primitiva para pegar os valores crus
                let (OI.OrderId id) = item.OrderId
                let (OI.Price preco) = item.Price
                let (OI.Quantity qtd) = item.Quantity
                let (OI.Tax taxa) = item.Tax
                
                // Matemática Financeira
                let receita = preco * (float qtd)
                let imposto = taxa * receita
                
                // Retorna uma Tupla: (Id do Pedido, Receita deste Item, Imposto deste Item)
                (id, receita, imposto)
            )

        // ========================================================================
        // ETAPA 3: A AGREGAÇÃO (Fold)
        // ========================================================================
        // Agora temos uma lista de Tuplas. Vários itens podem ter o mesmo Order ID.
        // O Fold vai amassar tudo isso em um dicionário agrupado por Order ID.
        
        let relatorioAgrupado =
            itensComReceita
            // O Fold recebe 1. Uma função, 2. Um estado inicial (Map vazio), 3. A lista implícita
            |> List.fold (fun acumulador (idDoPedido, receitaDoItem, impostoDoItem) -> 
                
                // Procuramos se já existe alguma soma iniciada para este ID na calculadora
                match Map.tryFind idDoPedido acumulador with
                | Some (somaReceita, somaImposto) -> 
                    // Já existe! Atualizamos o dicionário somando os valores velhos com os novos
                    let novaReceita = somaReceita + receitaDoItem
                    let novoImposto = somaImposto + impostoDoItem
                    Map.add idDoPedido (novaReceita, novoImposto) acumulador
                    
                | None -> 
                    // É a primeira vez que vemos este ID. Inserimos ele na calculadora.
                    Map.add idDoPedido (receitaDoItem, impostoDoItem) acumulador
                    
            ) Map.empty // O Estado Inicial do acumulador

        // O retorno da função é o dicionário 'relatorioAgrupado'
        relatorioAgrupado