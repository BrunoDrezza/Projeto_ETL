open FSharp.Data


[<EntryPoint>]
let main argv = 
    let filename = "/home/brunochapa/ProjetosFShap/Projeto_ETL/data/order.csv"
    let data = CsvFile.Load(filename)

    for row in data.Rows do
        printfn "%A" row

    0
