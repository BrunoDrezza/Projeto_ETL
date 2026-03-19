open System.IO
open FSharp.Data

// Importando seus módulos de domínio e parsers
open ETL.Order
open ETL.OrderItem
open ETL.ParserOrder
open ETL.ParserOrderItem
open ETL.ReportEngine

// ========================================================================
// 1. FUNÇÕES IMPURAS DE LEITURA (I/O)
// ========================================================================

/// Lê o arquivo CSV e retorna APENAS uma Lista de Records válidos (Requisito 5)
let carregarOrdens (caminhoArquivo: string) : Order list =
    let csv = CsvFile.Load(caminhoArquivo)
    
    csv.Rows
    // Dica Extra: Usar Seq.indexed ajuda a saber qual linha do CSV falhou!
    |> Seq.indexed 
    |> Seq.choose (fun (index, row) -> 
        match parseOrderSafe row with
        | Ok ordem -> Some ordem
        | Error mensagensDeErro -> 
            // Aqui entra a nossa lógica de fatiar e printar os erros!
            printfn "Falha ao carregar Ordem na linha %d do CSV:" (index + 1) // +1 porque o index começa em 1 e a linha 0 é o cabeçalho
            mensagensDeErro.Split(" | ")
            |> Array.iter (fun erro -> printfn "   -> %s" (erro.Trim()))
            printfn "---------------------------------------------------"
            
            None // Retorna None para ignorar essa linha e continuar a extração
    )
    |> Seq.toList

/// Faz a mesma coisa para os Itens do Pedido
let carregarItens (caminhoArquivo: string) : OrderItem list =
    let csv = CsvFile.Load(caminhoArquivo)
    
    csv.Rows
    |> Seq.indexed
    |> Seq.choose (fun (index, row) -> 
        match parseOrderItemSafe row with // Assumindo que você tem um parseOrderItemSafe similar
        | Ok item -> Some item
        | Error mensagensDeErro -> 
            printfn "Falha ao carregar OrderItem na linha %d do CSV:" (index + 1)
            mensagensDeErro.Split(" | ")
            |> Array.iter (fun erro -> printfn "   -> %s" (erro.Trim()))
            printfn "---------------------------------------------------"
            
            None
    )
    |> Seq.toList


// ========================================================================
// 2. FUNÇÃO IMPURA DE ESCRITA (I/O)
// ========================================================================

/// Recebe o Dicionário (Map) calculado pelo ReportEngine e salva no disco em formato CSV
let exportarRelatorioCsv (caminhoDestino: string) (dadosRelatorio: Map<int, float * float>) =
    
    // O cabeçalho exato que o gestor/professor pediu na imagem
    let cabecalho = "order_id,total_amount,total_taxes"

    // Transformando o Dicionário em uma Lista de Strings (uma string por linha)
    let linhasCsv =
        dadosRelatorio
        |> Map.toList // Transforma o Map em uma lista de Tuplas (Id, (Receita, Imposto))
        |> List.map (fun (orderId, (totalAmount, totalTaxes)) ->
            
            // Rigor de Formatação: 
            // %.2f garante que o número terá exatamente 2 casas decimais.
            // O F# usa InvariantCulture no sprintf por padrão, garantindo o ponto '.' e não vírgula.
            sprintf "%d,%.2f,%.2f" orderId totalAmount totalTaxes
        )

    // Junta o cabeçalho com as linhas de dados usando o operador de lista '::'
    let conteudoFinal = cabecalho :: linhasCsv

    // Comando do .NET para criar o arquivo e escrever todas as linhas de uma vez
    File.WriteAllLines(caminhoDestino, conteudoFinal)


// ========================================================================
// 3. A ROTINA PRINCIPAL (O Maestro)
// ========================================================================

[<EntryPoint>]
let main argv = 

    
    printfn " Iniciando Pipeline ETL (Insper)..."

    // 1. Resolvemos os caminhos de forma dinâmica
    let dirAtual = __SOURCE_DIRECTORY__
    let caminhoOrdens = Path.Combine(dirAtual, "..", "data", "order.csv")
    let caminhoItens = Path.Combine(dirAtual, "..", "data", "order_item.csv")
    let caminhoSaida = Path.Combine(dirAtual, "..", "data", "report_output.csv")

    // 2. Extração (Extract) - Impuro
    printfn "Carregando dados do disco..."
    let listaOrdens = carregarOrdens caminhoOrdens
    let listaItens = carregarItens caminhoItens

    printfn "   - %d Ordens válidas carregadas." listaOrdens.Length
    printfn "   - %d Itens válidos carregados." listaItens.Length

    // 3. Transformação (Transform) - 100% PURO
    printfn "Executando Motor de Cálculo (Status: Complete | Origem: Online)..."
    
    // Passamos os parâmetros
    let relatorioFinal = 
        calcularRelatorio Status.Complete Origin.Online listaOrdens listaItens

    // 4. Carga (Load / Export) - Impuro
    printfn "Exportando resultados para CSV..."
    exportarRelatorioCsv caminhoSaida relatorioFinal

    printfn "Sucesso! Arquivo gerado em:"
    printfn "   -> %s" caminhoSaida

    0 // Retorno Zero = Sem erros de sistema