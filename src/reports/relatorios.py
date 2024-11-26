import pandas as pd
from conexion.mongo_queries import MongoQueries

class RelatorioMongo:
    def __init__(self):
        pass

    def get_relatorio_funcionarios(self):
        mongo = MongoQueries()
        mongo.connect()
        query_result = mongo.db.FUNCIONARIOS.aggregate([
            {
                "$project": {
                    "codigo_funcionario": 1,
                    "nome": 1,
                    "cargo": 1,
                    "_id": 0
                }
            },
            {
                "$sort": {"nome": 1}  # Ordena por nome
            }
        ])
        
        df_funcionarios = pd.DataFrame(list(query_result))
        mongo.close()
        print(df_funcionarios)
        input("Pressione Enter para Sair do Relatório de Funcionários")

    def get_relatorio_pontos(self):
        mongo = MongoQueries()
        mongo.connect()
        query_result = mongo.db.PONTOS.aggregate([
            {
                "$project": {
                    "codigo_ponto": 1,
                    "data_ponto": 1,
                    "hora_entrada": {"$dateToString": {"format": "%H:%M", "date": "$hora_entrada"}},
                    "hora_saida": {"$dateToString": {"format": "%H:%M", "date": "$hora_saida"}},
                    "codigo_funcionario": 1,
                    "_id": 0
                }
            },
            {
                "$sort": {"codigo_funcionario": 1, "data_ponto": 1}  # Ordena por funcionário e data
            }
        ])

        df_pontos = pd.DataFrame(list(query_result))
        mongo.close()
        print(df_pontos)
        input("Pressione Enter para Sair do Relatório de Pontos")

    def get_relatorio_pontos_funcionarios(self):
        mongo = MongoQueries()
        mongo.connect()
        query_result = mongo.db.FUNCIONARIOS.aggregate([
            {
                "$lookup": {
                    "from": "PONTOS",
                    "localField": "codigo_funcionario",
                    "foreignField": "codigo_funcionario",
                    "as": "pontos"
                }
            },
            {
                "$project": {
                    "codigo_funcionario": 1,
                    "nome": 1,
                    "cargo": 1,
                    "dias_atraso": {
                        "$size": {
                            "$filter": {
                                "input": "$pontos",
                                "as": "p",
                                "cond": {"$gt": [{"$dateToString": {"format": "%H:%M", "date": "$$p.hora_entrada"}}, "08:00"]}
                            }
                        }
                    },
                    "horas_a_complementar": {
                        "$subtract": [
                            240,
                            {
                                "$divide": [
                                    {
                                        "$sum": {
                                            "$map": {
                                                "input": "$pontos",
                                                "as": "p",
                                                "in": {
                                                    "$subtract": [
                                                        {"$add": [{"$multiply": [{"$hour": "$$p.hora_saida"}, 60]}, {"$minute": "$$p.hora_saida"}]},
                                                        {"$add": [{"$multiply": [{"$hour": "$$p.hora_entrada"}, 60]}, {"$minute": "$$p.hora_entrada"}]}
                                                    ]
                                                }
                                            }
                                        }
                                    },
                                    60
                                ]
                            }
                        ]
                    },
                    "_id": 0
                }
            },
            {
                "$sort": {"nome": 1}  # Ordena por nome
            }
        ])
        df_pontos_funcionarios = pd.DataFrame(list(query_result))
        mongo.close()
        print(df_pontos_funcionarios)
        input("Pressione Enter para Sair do Relatório de Pontos por Funcionários")