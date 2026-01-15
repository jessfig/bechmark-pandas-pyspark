from enum import Enum


class TablesTPCH(Enum):
    CLIENTES = "customer"
    PEDIDOS = "orders"
    ITENS_PEDIDOS = "lineitem"
    PAISES = "nation"
