from enum import Enum


class TablesTPCH(Enum):
    CLIENTES = "customer"
    PEDIDOS = "orders"
    ITENS_PEDIDOS = "lineitem"
    PRODUTOS = "part"
    FORNECEDORES = "supplier"
    PRODUTOS_DOS_FORNECEDORES = "partsupp"
    PAISES = "nation"
    REGIOES = "region"
