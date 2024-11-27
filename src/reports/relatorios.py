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
            
            query_result = list(self.mongo.db.PONTOS.find(
                {}, 
                {
                    "codigo_ponto": 1,
                    "data_ponto": 1,
                    "hora_entrada": 1,
                    "hora_saida": 1,
                    "codigo_funcionario": 1,
                    "_id": 0
                }
            ).sort([("codigo_funcionario", 1), ("data_ponto", 1)]))
            
            for doc in query_result:
                if "hora_entrada" in doc and doc["hora_entrada"]:
                    doc["hora_entrada"] = doc["hora_entrada"].strftime("%H:%M")
                if "hora_saida" in doc and doc["hora_saida"]:
                    doc["hora_saida"] = doc["hora_saida"].strftime("%H:%M")
                if "data_ponto" in doc and doc["data_ponto"]:
                    doc["data_ponto"] = doc["data_ponto"].strftime("%d/%m/%Y")
        
            df_pontos = pd.DataFrame(list(query_result))
            mongo.close()
            print(df_pontos)
            input("Pressione Enter para Sair do Relatório de Pontos")
            
    def get_relatorio_pontos_funcionarios(self):
        try:
            self.mongo.connect()
            
            # Verificar se existem documentos nas coleções
            func_count = self.mongo.db.FUNCIONARIOS.count_documents({})
            pontos_count = self.mongo.db.PONTOS.count_documents({})
            
            if func_count == 0 or pontos_count == 0:
                print("\nUma ou ambas as coleções (FUNCIONARIOS/PONTOS) estão vazias.")
                return
            
            pipeline = [
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
                                    "cond": {
                                        "$gt": [
                                            {"$hour": "$$p.hora_entrada"},
                                            8
                                        ]
                                    }
                                }
                            }
                        },
                        "total_horas_trabalhadas": {
                            "$round": [
                                {
                                    "$divide": [
                                        {
                                            "$sum": {
                                                "$map": {
                                                    "input": "$pontos",
                                                    "as": "p",
                                                    "in": {
                                                        "$subtract": [
                                                            {"$add": [
                                                                {"$multiply": [{"$hour": "$$p.hora_saida"}, 60]},
                                                                {"$minute": "$$p.hora_saida"}
                                                            ]},
                                                            {"$add": [
                                                                {"$multiply": [{"$hour": "$$p.hora_entrada"}, 60]},
                                                                {"$minute": "$$p.hora_entrada"}
                                                            ]}
                                                        ]
                                                    }
                                                }
                                            }
                                        },
                                        60
                                    ]
                                },
                                2
                            ]
                        },
                        "_id": 0
                    }
                },
                {
                    "$sort": {"nome": 1}
                }
            ]
            
            query_result = list(self.mongo.db.FUNCIONARIOS.aggregate(pipeline))
            
            # Calcular horas a complementar (assumindo 240 horas mensais)
            for doc in query_result:
                doc['horas_a_complementar'] = round(240 - (doc['total_horas_trabalhadas'] or 0), 2)
                
            df_pontos_funcionarios = pd.DataFrame(query_result)
            self.check_data(df_pontos_funcionarios, "Pontos por Funcionários")
            
        except Exception as e:
            print(f"Erro ao gerar relatório de pontos por funcionários: {str(e)}")
        finally:
            self.mongo.close()
            input("\nPressione Enter para Sair do Relatório de Pontos por Funcionários")