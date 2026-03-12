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

