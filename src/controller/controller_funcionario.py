import pandas as pd
from model.funcionarios import Funcionario
from conexion.mongo_queries import MongoQueries

class Controller_Funcionario:
    def __init__(self):
        self.mongo = MongoQueries()
        
    def inserir_funcionario(self) -> Funcionario:
        # Cria uma nova conexão com o banco que permite alteração
        self.mongo.connect()
        
        # Solicita ao usuário o código do novo funcionário
        codigo_funcionario = input("Código do Funcionário (Novo): ")
        
        if self.verifica_existencia_funcionario(codigo_funcionario):
            # Solicita os dados do novo funcionário
            nome = input("Nome (Novo): ")
            cargo = input("Cargo (Novo): ")
            
            # Insere e persiste o novo funcionário
            self.mongo.db["funcionarios"].insert_one({
                "codigo_funcionario": codigo_funcionario,
                "nome": nome,
                "cargo": cargo
            })
            
            # Recupera os dados do novo funcionário criado transformando em um DataFrame
            df_funcionario = self.recupera_funcionario(codigo_funcionario)
            
            # Cria um novo objeto funcionário
            novo_funcionario = Funcionario(
                df_funcionario.codigo_funcionario.values[0],
                df_funcionario.nome.values[0],
                df_funcionario.cargo.values[0]
            )
            
            # Exibe os atributos do novo funcionário
            print("Funcionário criado com sucesso:")
            print(f"Código: {novo_funcionario.get_codigo_funcionario()}")
            print(f"Nome: {novo_funcionario.get_nome()}")
            print(f"Cargo: {novo_funcionario.get_cargo()}")
            
            self.mongo.close()
            return novo_funcionario
        else:
            self.mongo.close()
            print(f"O código {codigo_funcionario} já está cadastrado.")
            return None

    def atualizar_funcionario(self) -> Funcionario:
        # Cria uma nova conexão com o banco que permite alteração
        self.mongo.connect()

        # Solicita ao usuário o código do funcionário a ser alterado
        codigo_funcionario = input("Código do Funcionário que deseja atualizar: ")

        # Verifica se o funcionário existe na base de dados
        if not self.verifica_existencia_funcionario(codigo_funcionario):
            # Solicita os novos dados do funcionário
            nome = input("Nome (Novo): ")
            cargo = input("Cargo (Novo): ")
            
            # Atualiza os dados do funcionário
            self.mongo.db["funcionarios"].update_one(
                {"codigo_funcionario": codigo_funcionario},
                {"$set": {"nome": nome, "cargo": cargo}}
            )
            
            # Recupera os dados do funcionário atualizado
            df_funcionario = self.recupera_funcionario(codigo_funcionario)
            
            # Cria um objeto com os dados atualizados
            funcionario_atualizado = Funcionario(
                df_funcionario.codigo_funcionario.values[0],
                df_funcionario.nome.values[0],
                df_funcionario.cargo.values[0]
            )
            
            # Exibe os atributos do funcionário atualizado
            print("Funcionário atualizado com sucesso:")
            print(f"Código: {funcionario_atualizado.get_codigo_funcionario()}")
            print(f"Nome: {funcionario_atualizado.get_nome()}")
            print(f"Cargo: {funcionario_atualizado.get_cargo()}")
            
            self.mongo.close()
            return funcionario_atualizado
        else:
            self.mongo.close()
            print(f"O código {codigo_funcionario} não existe.")
            return None

    def excluir_funcionario(self):
        # Cria uma nova conexão com o banco que permite alteração
        self.mongo.connect()

        # Solicita ao usuário o código do funcionário a ser excluído
        codigo_funcionario = input("Código do Funcionário que irá excluir: ")

        # Verifica se o funcionário existe na base de dados
        if not self.verifica_existencia_funcionario(codigo_funcionario):
            # Primeiro exclui os pontos relacionados ao funcionário
            self.mongo.db["pontos"].delete_many({"codigo_funcionario": codigo_funcionario})
            
            # Recupera os dados do funcionário antes de excluir
            df_funcionario = self.recupera_funcionario(codigo_funcionario)
            
            # Remove o funcionário
            self.mongo.db["funcionarios"].delete_one({"codigo_funcionario": codigo_funcionario})
            
            # Cria um objeto com os dados do funcionário excluído
            funcionario_excluido = Funcionario(
                df_funcionario.codigo_funcionario.values[0],
                df_funcionario.nome.values[0],
                df_funcionario.cargo.values[0]
            )
            
            print("Funcionário e seus pontos relacionados foram removidos com sucesso!")
            print(f"Código: {funcionario_excluido.get_codigo_funcionario()}")
            print(f"Nome: {funcionario_excluido.get_nome()}")
            print(f"Cargo: {funcionario_excluido.get_cargo()}")
            
            self.mongo.close()
        else:
            self.mongo.close()
            print(f"O código {codigo_funcionario} não existe.")

    def verifica_existencia_funcionario(self, codigo_funcionario: str, external: bool = False) -> bool:
        if external:
            self.mongo.connect()

        # Verifica se existe um funcionário com o código informado
        df_funcionario = pd.DataFrame(
            self.mongo.db["funcionarios"].find(
                {"codigo_funcionario": codigo_funcionario},
                {"codigo_funcionario": 1, "nome": 1, "cargo": 1, "_id": 0}
            )
        )

        if external:
            self.mongo.close()

        return df_funcionario.empty

    def recupera_funcionario(self, codigo_funcionario: str, external: bool = False) -> pd.DataFrame:
        if external:
            self.mongo.connect()

        # Recupera os dados do funcionário
        df_funcionario = pd.DataFrame(
            list(self.mongo.db["funcionarios"].find(
                {"codigo_funcionario": codigo_funcionario},
                {"codigo_funcionario": 1, "nome": 1, "cargo": 1, "_id": 0}
            ))
        )

        if external:
            self.mongo.close()

        return df_funcionario