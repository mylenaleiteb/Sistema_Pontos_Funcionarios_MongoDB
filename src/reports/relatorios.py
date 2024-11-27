import pandas as pd
from conexion.mongo_queries import MongoQueries

class RelatorioMongo:
    def __init__(self):
        self.mongo = MongoQueries()
    
    def check_data(self, df, report_name):
        if df.empty:
            print(f"\nNenhum dado encontrado para o relatório de {report_name}")
        else:
            print(f"\nRelatório de {report_name}:")
            print(df.to_string(index=False))
            print(f"\nTotal de registros: {len(df)}")

    def get_relatorio_funcionarios(self):
        mongo = MongoQueries()
        mongo.connect()
            
        query_result = mongo.db["funcionarios"].find({},
            {
                "codigo_funcionario": 1,
                "nome": 1,
                "cargo": 1,
                "_id": 0
            }
        ).sort("nome", 1)
            
        df_funcionarios = pd.DataFrame(list(query_result))
        mongo.close()
        print(df_funcionarios)
        input("Pressione Enter para Sair do Relatório de Funcionarios")
        
                
    def get_relatorio_pontos(self):
            mongo = MongoQueries()
            mongo.connect()
            
            query_result = mongo.db["pontos"].find({}, 
                {
                    "codigo_ponto": 1,
                    "data_ponto": 1,
                    "hora_entrada": 1,
                    "hora_saida": 1,
                    "codigo_funcionario": 1,
                    "_id": 0
                }
            ).sort([("codigo_funcionario", 1), ("data_ponto", 1)])
        
            df_pontos = pd.DataFrame(list(query_result))
            mongo.close()
            print(df_pontos)
            input("Pressione Enter para Sair do Relatório de Pontos")
                
    def get_relatorio_pontos_funcionarios(self):
        try:
            # Conectar ao MongoDB
            mongo = MongoQueries()
            mongo.connect()
            
            # Verificar se as coleções têm documentos
            func_count = mongo.db["funcionarios"].count_documents({})
            pontos_count = mongo.db["pontos"].count_documents({})
            
            if func_count == 0 or pontos_count == 0:
                print("\nUma ou ambas as coleções (FUNCIONARIOS/PONTOS) estão vazias.")
                return
                
            # Pipeline para agregar dados de funcionários e pontos
            pipeline = [
                {
                    "$lookup": {
                        "from": "pontos",
                        "localField": "codigo_funcionario",
                        "foreignField": "codigo_funcionario",
                        "as": "pontos"
                    }
                },
                {
                    "$project": {
                        "_id": 0,  # Excluir o campo _id
                        "codigo_funcionario": 1,
                        "nome": 1,
                        "cargo": 1,
                        "dias_atraso": {
                            "$size": {
                                "$filter": {
                                    "input": "$pontos",
                                    "as": "p",
                                    "cond": {
                                        "$let": {
                                            "vars": {
                                                "hora": {
                                                    "$substr": ["$$p.hora_entrada", 0, 2]
                                                }
                                            },
                                            "in": {
                                                "$gt": [
                                                    {"$toInt": "$$hora"},
                                                    8
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        "total_horas_trabalhadas": {
                            "$round": [
                                {
                                    "$sum": {
                                        "$map": {
                                            "input": "$pontos",
                                            "as": "p",
                                            "in": {
                                                "$let": {
                                                    "vars": {
                                                        "entrada_hora": {"$toInt": {"$substr": ["$$p.hora_entrada", 0, 2]}},
                                                        "entrada_min": {"$toInt": {"$substr": ["$$p.hora_entrada", 3, 2]}},
                                                        "saida_hora": {"$toInt": {"$substr": ["$$p.hora_saida", 0, 2]}},
                                                        "saida_min": {"$toInt": {"$substr": ["$$p.hora_saida", 3, 2]}}
                                                    },
                                                    "in": {
                                                        "$divide": [
                                                            {
                                                                "$add": [
                                                                    {
                                                                        "$multiply": [
                                                                            {
                                                                                "$subtract": ["$$saida_hora", "$$entrada_hora"]
                                                                            },
                                                                            60
                                                                        ]
                                                                    },
                                                                    {
                                                                        "$subtract": ["$$saida_min", "$$entrada_min"]
                                                                    }
                                                                ]
                                                            },
                                                            60
                                                        ]
                                                    }
                                                }
                                            }
                                        }
                                    }
                                },
                                2
                            ]
                        }
                    }
                },
                {
                    "$addFields": {
                        "horas_a_complementar": {
                            "$round": [
                                {
                                    "$subtract": [
                                        240,  # Total de horas mensais esperadas
                                        "$total_horas_trabalhadas"
                                    ]
                                },
                                2
                            ]
                        }
                    }
                },
                {
                    "$sort": {"nome": 1}
                }
            ]
            
            # Executar a agregação
            query_result = list(mongo.db["funcionarios"].aggregate(pipeline))
            
            # Converter o resultado em um DataFrame
            df_relatorio = pd.DataFrame(query_result)
            
            # Verificar e exibir o DataFrame
            self.check_data(df_relatorio, "Relatório de Pontos por Funcionário")
            
        except Exception as e:
            print(f"Erro ao gerar relatório de pontos por funcionário: {str(e)}")
        finally:
            mongo.close()
            input("\nPressione Enter para Sair do Relatório de Pontos por Funcionário")