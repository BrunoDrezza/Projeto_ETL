namespace ETL


open FSharp.Data
open System
open System.Globalization



module private ResultEx = 
    /// <summary>
    /// Implementação de um Applicative Functor para 5 parâmetros.
    /// Estratégia de Acúmulo de Erros (Error Accumulation): Avalia múltiplos Results simultaneamente.
    /// Se todos forem sucesso, monta o objeto final (Order ou OrderItem).
    /// Se houver falhas, coleta e concatena TODOS os erros de uma vez, otimizando o ciclo de debug do ETL.
    /// <summary>
    
    let map5 f r1 r2 r3 r4 r5 = 
        
        // Avalia o estado dos 5 trilhos de validação ao mesmo tempo
        match r1, r2, r3, r4, r5 with
        
        // 1. CAMINHO FELIZ (Happy Path)
        // Se todas as 5 caixas tiverem o rótulo 'Ok', desembrulhamos os valores puros (a, b, c, d, e)
        // e os passamos para a função construtora 'f', re-embalando o resultado em um novo 'Ok'.
        | Ok a, Ok b, Ok c, Ok d, Ok e -> Ok (f a b c d e)
        
        // 2. CAMINHO DE FALHA (Error Path)
        // Se pelo menos um Result for 'Error', caímos aqui para coletar os estragos.
        | _ -> 
            [
                // Passo A: Normalização de Tipos
                // Como as listas no F# exigem elementos do mesmo tipo, ignoramos os valores de sucesso
                // transformando-os em 'unit' (). Os valores de Error permanecem intactos.
                // Assinatura resultante da lista: Result<unit, string> list
                r1 |> Result.map (fun _ -> ())
                r2 |> Result.map (fun _ -> ())
                r3 |> Result.map (fun _ -> ())
                r4 |> Result.map (fun _ -> ())
                r5 |> Result.map (fun _ -> ())
            ] 
            
            // Passo B: Filtragem e Extração
            // List.choose avalia cada item:
            // - Se for 'Ok', retorna 'None' (Descarta o item da lista final)
            // - Se for 'Error err', retorna 'Some err' (Extrai a mensagem de erro e a mantém na lista)
            |> List.choose (function 
                | Error err -> Some err
                | Ok _ -> None
            )
            
            // Passo C: Agregação
            // Une a lista limpa de strings de erro em uma única mensagem consolidada.
            // Exemplo: "ID inválido | Preço não pode ser negativo"
            |> String.concat " | "
            
            // Passo D: Re-empacotamento de Segurança
            // O construtor 'Error' atua como uma função (string -> Result<'a, string>).
            // Ele recebe a string consolidada e a devolve para o trilho de falhas do pipeline principal.
            |> Error

module private Parse =

    
    // Esse módulo guarda as funções que vão transformar as strings do csv nos "types" padrões do F# que posteriormente vamos
    // usar para transformar nos tipos específicos garantindo máxima rigidez de uma programação funcional.

    // OBS : Módulo privado não é importado para outros arquivos quando chamamamos o ETL, serve para não deixar o código poluído
    // , principalmente quando não vamos usar esse módulo em outro lugar que não na parte de parsing para transformar as
    // linhas do csv de string para os valores nos quais poderemos fazer as contas

    

    let getColumn (row: CsvRow) (columnName: string) : Result<string, string> =

        
        // Essa função recebe 2 valores, a linha que estamos tentando transformar e o nome da coluna, para conseguirmos
        // "filtrar" exatamente o elemento específico desse csv que queremos. Caso ele retorne algum valor
        // A função tenta puxar por um valor específico caso ele não retorne nada, o código não quebra,
        // ele retorna um erro que depois vai ser guardado em uma lista para futuro debugging

        // Result é o que chamamos de União Discriminada, ele é usado para tratamento de erros funcional
        // permitindo que uma função retorne um resultado de sucesso ou de erro de forma explicita
        // Ao contrário de exceções tradicionais que quebram o fluxo de execução o Result força o chamador
        // a lidar com o cenário de falha
    
        try 
            Ok (row.GetColumn(columnName))
        with 
            |ex -> 

            
            // O ex ele é um Objeto da Classe System.Exception que serve para ajudar a debbugar o código
            // ex.Message -> retorna uma descrição curta de erro. Mas temos também outras funcionalidades,
            // como por exemplo o ex.StackTrace -> Retorna a linha exata do código que deu esse erro.
            // Entre outras coisas
            
            
                let erroDetalhado = sprintf "Erro na Coluna: '%s' | Erro : %s | Local : '%s'" columnName ex.Message ex.StackTrace
                Error erroDetalhado

    
    let toInt (fieldName : string) (raw: string) : Result<int, string> =
        
        // System.Int32 é um tipo de dado primitivo do F# quando você escreve int você está usando um
        // apelido para o Int32

        // 32 bits = 4 bytes
        // desses 32 espaços que contem o valor 0 ou 1, desses 32 espaços 1 bit é reservado para o sinal positivo ou negativo
        // e os outros 32 bits são para o valor numérico do número inteiro
        // se quisessemos usar mais espaço, para um número muito grande, teriamos que usar System.Int128 por exemplo

        // Usamos Int32.TryParse por que o Int32 como objeto do System possuí métodos inteligentes
        // Já o int sozinho não, ele é uma função tudo ou nada, se der algum tipo de erro ele levanta uma exceção
        // e com essa exceção o código para de funcionar, não permitindo rodar o código todo e debuggar tudo de uma vez
        // O TryParse do Int32 ele é esperto, por que ele retorna uma tupla, com (Bool, valor) esse bool define
        // se a transformação funcionou, se funcionou retorna true e o valor se não ele retorna false com uma exceção
        // que você pode guardar, e usar depois sem quebrar o código no meio 

        match Int32.TryParse(raw.Trim()) with
            | true, inteiro -> Ok inteiro
            | false, _ -> Error (sprintf "Campo '%s' invalido para int: '%s'" fieldName raw)

    // Função para converter texto em número decimal (float/double)
    let toFloat (fieldName: string) (raw: string) : Result<float, string> =
        
        // ENTENDENDO O DOUBLE.TRYPARSE:
        // Por que 'Double' se o tipo no F# é 'float'? 
        //    No F#, 'float' é apenas um apelido para 'System.Double' do .NET. 
        //    É um número de precisão dupla (64 bits)
        //
        // O que ele faz?
        //    Diferente da função 'float "10.5"' (que explode se o texto for "ABC"), o TryParse 
        //    é um inspetor seguro. Ele testa a string e retorna uma Tupla de dois valores: 
        //    (SucessoOuFalha : bool, NumeroConvertido : float).
        //
        //  Os Parâmetros de Rigor:
        //    - NumberStyles.Float: Diz ao inspetor: "Aceite números com ponto (10.5) e notação científica (1e-6), mas recuse letras".
        //    - CultureInfo.InvariantCulture: O escudo de globalização. Garante que o sistema SEMPRE leia 
        //      o ponto '.' como separador de decimais, ignorando se o Windows do usuário está em Português (onde seria vírgula).
        
        match Double.TryParse(raw.Trim(), NumberStyles.Float, CultureInfo.InvariantCulture) with
        | true, value -> Ok value  // Se o bool for true, empacota o 'value' (o número real) no Ok.
        | false, _ -> Error (sprintf "Campo '%s' invalido para float: '%s'" fieldName raw)
    
    
    // Função para converter texto em Data e Hora com precisão cirúrgica
    let toDateTime (fieldName: string) (raw: string) : Result<DateTime, string> =
        let formats = [| "yyyy-MM-ddTHH:mm:ss"; "yyyy-MM-ddTHH:mm:ssZ" |]
        
        // ENTENDENDO O TRYPARSE EXACT E O OPERADOR ||| (Bitwise OR):
        //
        // O que é o TryParseExact?
        //    A função TryParse comum do .NET é "boazinha". Ela tenta adivinhar a data. 
        //    Se você mandar "10/11/2026", ela pode ler como Outubro ou Novembro dependendo do PC.
        //    O 'Exact' é o Rigor Máximo. Ele diz: "Ou a string está EXATAMENTE igual aos 'formats'
        //    que eu passei (ISO 8601 com o 'T' no meio), ou eu recuso a entrada". Zero adivinhações.
        //
        // O que é o operador ||| (Bitwise OR / OU Binário)?
        //    Configurações como DateTimeStyles são guardadas na memória como números binários (Flags).
        //    Imagine que o .NET tem um painel de disjuntores (0 = desligado, 1 = ligado):
        //
        //    - AssumeUniversal   (Disjuntor A): Binário 00100000 (Ignora fuso local, assume que a string é UTC)
        //    - AdjustToUniversal (Disjuntor B): Binário 00010000 (Garante que a saída do DateTime seja UTC)
        //
        //    Como passar duas configurações ao mesmo tempo para a função? 
        //    Usamos o ||| (OU Binário). Ele sobrepõe os números na memória:
        //      00100000 (Assume)
        //    + 00010000 (Adjust)
        //    -------------------
        //    = 00110000 (O painel final com os DOIS disjuntores ligados)
        //
        //    O F# passa esse número único consolidado para a função, ativando as duas proteções de fuso horário.

        match DateTime.TryParseExact(raw.Trim(), formats, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal ||| DateTimeStyles.AdjustToUniversal) with
        | true, value -> Ok value
        | false, _ -> Error (sprintf "Campo '%s' invalido para data: '%s'" fieldName raw)


module ParserOrder =
    module O = ETL.Order // Alias para encurtar o caminho dos tipos e facilitar entendimento
    
     // O que eu preciso transformar?
     
    (*id, client_id,  order_date,         status,  origin
    [|"1"; "112"; "2024-10-02T03:05:39"; "Pending"; "P"|]
    [|"2"; "117"; "2024-08-17T03:05:39"; "Complete"; "O"|]
    [|"3"; "120"; "2024-09-10T03:05:39"; "Cancelled"; "O"|]
    [|"4"; "116"; "2024-03-11T03:05:39"; "Pending"; "O"|]
    [|"5"; "103"; "2024-06-05T03:05:39"; "Complete"; "O"|]
    [|"6"; "100"; "2024-04-18T03:05:39"; "Pending"; "O"|]
    [|"7"; "101"; "2024-06-28T03:05:39"; "Cancelled"; "O"|]
    [|"8"; "102"; "2024-11-07T03:05:39"; "Complete"; "O"|]
    [|"9"; "115"; "2025-01-08T03:05:39"; "Pending"; "O"|]
    [|"10"; "119"; "2024-05-03T03:05:39"; "Pending"; "P"|]
    [|"11"; "119"; "2024-11-29T03:05:39"; "Complete"; "O"|]   
    *)

    // Dar match no pending e no P e O pra online e Presencial
    // O Resto só preciso tranformar normalmente, mas de maneira rigorosa a ponto de tornar impossível cometer algum engano na estrutura de dados

    // O 'Result.map O.Id' pega o 'int' de dentro do Ok e embrulha no tipo OrderId
    let parseId (raw: string) : Result<O.Id, string> =
        Parse.toInt "id" raw |> Result.map O.Id

    let parseClientId (raw: string) : Result<O.ClientId, string> =
        Parse.toInt "client_id" raw |> Result.map O.ClientId

    let parseOrderDate (raw: string) : Result<O.OrderDate, string> =
        Parse.toDateTime "order_date" raw |> Result.map O.OrderDate

    // Pattern matching puro para converter string em Enum/Discriminated Union
    let parseStatus (raw: string) : Result<O.Status, string> =
        match raw.Trim().ToLowerInvariant() with
        | "pending" -> Ok O.Status.Pending
        | "complete" -> Ok O.Status.Complete
        | "cancelled" -> Ok O.Status.Cancelled
        | other -> Error (sprintf "Status desconhecido: '%s'" other)

    // parseOrigin funciona exatamente como o Status, tratando variações de texto
    let parseOrigin (raw: string) : Result<O.Origin, string> =
        match raw.Trim().ToLowerInvariant() with
        | "p" -> Ok O.Origin.Presencial
        | "o" -> Ok O.Origin.Online
        | other -> Error (sprintf "Origem desconhecida: '%s'" other)

// VERSÃO SEGURA: Retorna Result. Se falhar, temos a lista de erros.
    let parseOrderSafe (row: CsvRow) : Result<O.Order, string> =
        // Usamos bind para encadear: 1. Pega Coluna -> 2. Tenta converter
        // Se a coluna não existir, o bind nem tenta rodar o parseId
        let idR = Parse.getColumn row "id" |> Result.bind parseId
        let clientIdR = Parse.getColumn row "client_id" |> Result.bind parseClientId
        let orderDateR = Parse.getColumn row "order_date" |> Result.bind parseOrderDate
        let statusR = Parse.getColumn row "status" |> Result.bind parseStatus
        let originR = Parse.getColumn row "origin" |> Result.bind parseOrigin

        // O map5 junta os 5 Results. Se todos forem Ok, monta o Record { ... }
        ResultEx.map5
            (fun id clientId orderDate status origin ->
                { Id = id; ClientId = clientId; OrderDate = orderDate; Status = status; Origin = origin })
            idR clientIdR orderDateR statusR originR

    // usa a parseOrderSafe e Se der erro, ela PARA o sistema (Exception)
    let parseOrder (row: CsvRow) : O.Order =
        match parseOrderSafe row with
        | Ok order -> order
        | Error err -> invalidOp err // Lança InvalidOperationException com o relatório de erros


module ParserOrderItem =

    // O que eu preciso transformar aqui??
    (*
    order_id,product_id,quantity,price, tax
    12,      224,       8,      139.42, 0.12
    13,      213,       1,      160.6, 0.16
    2,       203,       7,      110.37, 0.15
    16,      223,       3,      195.11, 0.17
    20,      227,       1,      142.84, 0.09
    *)

    // Nesse eu não preciso dar o Match com nenhum deles mas para garantir o rigor da programação funcional eu vou transformar cada elemento em um tipo que eu criei

    // ALIAS DE MÓDULO (Domain Isolation)
    // No meu código or 'OrderItem' pode ser o nome do Tipo E do Módulo. 
    // Criamos o alias 'OI' para evitar ambiguidade e reduzir o "ruído" visual no código.
    module OI = ETL.OrderItem

    // FUNÇÕES DE ELEVAÇÃO (Lifting Functions)
    // Estas funções seguem o padrão: String -> Result<DomainType, string>
    
    let parseOrderId (raw: string) : Result<OI.OrderId, string> =
        // Parse.toInt: Tenta transformar string em int puro (Result<int, string>)
        // Result.map: Pega o construtor 'OI.OrderId' (que é uma função int -> OrderId) 
        // e o "eleva" para funcionar dentro da caixa Result.
        // Se toInt retornar Error, o Result.map nem é executado.
        Parse.toInt "order_id" raw |> Result.map OI.OrderId

    let parseProductId (raw: string) : Result<OI.ProductId, string> =
        Parse.toInt "product_id" raw |> Result.map OI.ProductId

    let parseQuantity (raw: string) : Result<OI.Quantity, string> =
        Parse.toInt "quantity" raw |> Result.map OI.Quantity

    let parsePrice (raw: string) : Result<OI.Price, string> =
        // Aqui usamos 'toFloat' porque preços no mercado financeiro exigem decimais.
        // O rigor aqui garante que "10.50" não seja lido como "1050" por erro de cultura.
        Parse.toFloat "price" raw |> Result.map OI.Price

    let parseTax (raw: string) : Result<OI.Tax, string> =
        Parse.toFloat "tax" raw |> Result.map OI.Tax

    // 3. O AGREGADOR DE RESULTADOS (The Safe Parser)
    let parseOrderItemSafe (row: CsvRow) : Result<OI.OrderItem, string> =
        // Mecânica do Result.bind:
        // getColumn retorna Result<string, string>. 
        // bind 'abre' esse Result e, se for Ok, passa a string para 'parseOrderId'.
        // Isso cria uma "corrente de segurança": se a coluna não existir, o parser nem tenta converter.
        let orderIdR = Parse.getColumn row "order_id" |> Result.bind parseOrderId
        let productIdR = Parse.getColumn row "product_id" |> Result.bind parseProductId
        let quantityR = Parse.getColumn row "quantity" |> Result.bind parseQuantity
        let priceR = Parse.getColumn row "price" |> Result.bind parsePrice
        let taxR = Parse.getColumn row "tax" |> Result.bind parseTax

        // Aplicação da Função ResultEx.map5:
        // Esta é uma implementação de um 'Applicative Functor'.
        // Ela recebe 5 argumentos do tipo Result e uma função anônima (lambda).
        ResultEx.map5
            // Esta Lambda só roda se r1 até r5 forem todos 'Ok'.
            // Ela recebe os valores já "desembrulhados" (puros) e monta o Record.
            (fun orderId productId quantity price tax ->
                {
                    OrderId = orderId
                    ProductId = productId
                    Quantity = quantity
                    Price = price
                    Tax = tax
                })
            // Os 5 trilhos de dados que serão validados em paralelo pelo map5
            orderIdR productIdR quantityR priceR taxR

    // 4. A PONTE PARA O MUNDO IMPURO (The Unsafe Wrapper)
    let parseOrderItem (row: CsvRow) : OI.OrderItem =
        // Aqui decidimos o que acontece no final do pipeline de ETL.
        match parseOrderItemSafe row with
        | Ok item -> item // Se o dado passou em todos os testes, entregamos o objeto limpo.
        | Error err -> 
            // invalidOp: No rigor máximo, se o dado chegou aqui 'podre', 
            // lançamos uma InvalidOperationException. 
            // Isso interrompe o processo para evitar que um preço errado chegue ao banco.
            invalidOp err
