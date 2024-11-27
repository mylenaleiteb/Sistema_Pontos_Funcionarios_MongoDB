from datetime import datetime
import pandas as pd
from model.funcionarios import Funcionario
from model.pontos import Ponto
from controller.controller_funcionario import Controller_Funcionario
from conexion.mongo_queries import MongoQueries

class Controller_Ponto:
    def __init__(self):
        self.ctrl_funcionario = Controller_Funcionario()
        self.mongo = MongoQueries()
        
    def inserir_ponto(self) -> Ponto:
        # Cria uma nova conexão com o banco
        self.mongo.connect()

        try:
            codigo_funcionario = input("Código do Funcionário: ")
        except ValueError:
            print("Entrada inválida. O código do funcionário deve ser um número inteiro.")
            return None
        
        # Verifica se o funcionário existe
        if not self.ctrl_funcionario.verifica_existencia_funcionario(codigo_funcionario, external=True):
            # Busca os dados do funcionário
            df_funcionario = self.ctrl_funcionario.recupera_funcionario(codigo_funcionario, external=True)
            
            try:
                data_ponto = input("Data (YYYY-MM-DD): ")
                hora_entrada = input("Hora de Entrada (HH:MM): ")
                hora_saida = input("Hora de Saída (HH:MM): ")

                # Validar os formatos de data e hora
                datetime.strptime(data_ponto, '%Y-%m-%d')
                datetime.strptime(hora_entrada, '%H:%M')
                datetime.strptime(hora_saida, '%H:%M')
            except ValueError:
                print("Data ou hora inválida. Por favor, insira os dados no formato correto.")
                self.mongo.close()
                return None

            # Recupera o próximo código disponível
            max_ponto = self.mongo.db['pontos'].find_one(sort=[('codigo_ponto', -1)])
            proximo_codigo = '1' if not max_ponto else str(int(max_ponto['codigo_ponto']) + 1)

            # Insere o novo ponto
            self.mongo.db['pontos'].insert_one({
                'codigo_ponto': proximo_codigo,
                'data_ponto': data_ponto,
                'hora_entrada': hora_entrada,
                'hora_saida': hora_saida,
                'codigo_funcionario': codigo_funcionario
            })

            # Cria o objeto Funcionario
            funcionario = Funcionario(
                df_funcionario.codigo_funcionario.values[0],
                df_funcionario.nome.values[0],
                df_funcionario.cargo.values[0]
            )
            
            # Cria o objeto Ponto
            novo_ponto = Ponto(proximo_codigo, data_ponto, hora_entrada, hora_saida, funcionario)
            
            print("Ponto registrado com sucesso:")
            print(f"Código: {novo_ponto.get_codigo_ponto()}")
            print(f"Data: {novo_ponto.get_data_ponto()}")
            print(f"Entrada: {novo_ponto.get_hora_entrada()}")
            print(f"Saída: {novo_ponto.get_hora_saida()}")
            print(f"Funcionário: {novo_ponto.get_funcionario().get_nome()}")
            
            self.mongo.close()
            return novo_ponto
        else:
            print(f"O código {codigo_funcionario} não existe.")
            self.mongo.close()
            return None

    def atualizar_ponto(self) -> Ponto:
        self.mongo.connect()

        try:
            codigo_ponto = input("Código do Ponto que irá alterar: ")
        except ValueError:
            print("Entrada inválida. O código do ponto deve ser um número inteiro.")
            return None

        if not self.verifica_existencia_ponto(codigo_ponto):
            try:
                nova_data = input("Nova Data (YYYY-MM-DD): ")
                nova_hora_entrada = input("Nova Hora de Entrada (HH:MM): ")
                nova_hora_saida = input("Nova Hora de Saída (HH:MM): ")

                # Validar os formatos de data e hora
                datetime.strptime(nova_data, '%Y-%m-%d')
                datetime.strptime(nova_hora_entrada, '%H:%M')
                datetime.strptime(nova_hora_saida, '%H:%M')
            except ValueError:
                print("Data ou hora inválida. Por favor, insira os dados no formato correto.")
                self.mongo.close()
                return None

            # Atualiza o ponto
            self.mongo.db['pontos'].update_one(
                {'codigo_ponto': codigo_ponto},
                {'$set': {
                    'data_ponto': nova_data,
                    'hora_entrada': nova_hora_entrada,
                    'hora_saida': nova_hora_saida
                }}
            )

            # Recupera o ponto atualizado
            ponto_atualizado = self.mongo.db['pontos'].find_one({'codigo_ponto': codigo_ponto})
            
            # Recupera os dados do funcionário
            df_funcionario = self.ctrl_funcionario.recupera_funcionario(
                ponto_atualizado['codigo_funcionario'], 
                external=True
            )

            # Cria o objeto Funcionario
            funcionario = Funcionario(
                df_funcionario.codigo_funcionario.values[0],
                df_funcionario.nome.values[0],
                df_funcionario.cargo.values[0]
            )

            # Cria o objeto Ponto atualizado
            ponto = Ponto(
                ponto_atualizado['codigo_ponto'],
                ponto_atualizado['data_ponto'],
                ponto_atualizado['hora_entrada'],
                ponto_atualizado['hora_saida'],
                funcionario
            )

            print("Ponto atualizado com sucesso:")
            print(f"Código: {ponto.get_codigo_ponto()}")
            print(f"Data: {ponto.get_data_ponto()}")
            print(f"Entrada: {ponto.get_hora_entrada()}")
            print(f"Saída: {ponto.get_hora_saida()}")
            print(f"Funcionário: {ponto.get_funcionario().get_nome()}")

            self.mongo.close()
            return ponto
        else:
            print(f"O código {codigo_ponto} não existe.")
            self.mongo.close()
            return None

    def excluir_ponto(self):
        self.mongo.connect()

        try:
            codigo_ponto = input("Código do Ponto que irá excluir: ")
        except ValueError:
            print("Entrada inválida. O código do ponto deve ser um número inteiro.")
            return None

        if not self.verifica_existencia_ponto(codigo_ponto):
            # Recupera os dados do ponto antes de excluir
            ponto = self.mongo.db['pontos'].find_one({'codigo_ponto': codigo_ponto})
            
            # Recupera os dados do funcionário
            df_funcionario = self.ctrl_funcionario.recupera_funcionario(
                ponto['codigo_funcionario'], 
                external=True
            )

            # Remove o ponto
            self.mongo.db['pontos'].delete_one({'codigo_ponto': codigo_ponto})

            # Cria o objeto Funcionario
            funcionario = Funcionario(
                df_funcionario.codigo_funcionario.values[0],
                df_funcionario.nome.values[0],
                df_funcionario.cargo.values[0]
            )

            # Cria o objeto do ponto excluído para exibição
            ponto_excluido = Ponto(
                ponto['codigo_ponto'],
                ponto['data_ponto'],
                ponto['hora_entrada'],
                ponto['hora_saida'],
                funcionario
            )

            print("Ponto removido com sucesso:")
            print(f"Código: {ponto_excluido.get_codigo_ponto()}")
            print(f"Data: {ponto_excluido.get_data_ponto()}")
            print(f"Entrada: {ponto_excluido.get_hora_entrada()}")
            print(f"Saída: {ponto_excluido.get_hora_saida()}")
            print(f"Funcionário: {ponto_excluido.get_funcionario().get_nome()}")
            
            self.mongo.close()
        else:
            print(f"O código {codigo_ponto} não existe.")
            self.mongo.close()

    def verifica_existencia_ponto(self, codigo_ponto: str) -> bool:
        # Verifica se existe um ponto com o código informado
        df_ponto = pd.DataFrame(
            self.mongo.db['pontos'].find(
                {'codigo_ponto': codigo_ponto},
                {'codigo_ponto': 1, '_id': 0}
            )
        )
        return df_ponto.empty