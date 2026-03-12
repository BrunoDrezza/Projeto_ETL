namespace ETL

module Order =
    type Status =
        | Pending
        | Complete
        | Cancelled
    type Origin =
        | Presencial
        | Online
    type Id = int
    type OrderDate = System.DateTime
    type ClientId = int

    type Order = {
        Id: Id
        Origin: Origin
        Status: Status
        OrderDate: OrderDate
        ClientId: ClientId
    }

module OrderItem = 

    type OrderId = int
    type ProductId = int
    type Quantity = int
    type Price = float
    type Tax = float

    type OrderItem = {
        OrderId: OrderId
        ProductId: ProductId
        Quantity: Quantity
        Price: Price
        Tax: Tax
    }
