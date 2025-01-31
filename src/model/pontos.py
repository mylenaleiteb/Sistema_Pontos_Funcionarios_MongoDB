import pandas as pd
from datetime import datetime

class Ponto:
    def __init__(self, codigo_ponto, data_ponto, hora_entrada, hora_saida, funcionario):
        self.__codigo_ponto = codigo_ponto
        self.__data_ponto = data_ponto
        self.__hora_entrada = hora_entrada
        self.__hora_saida = hora_saida
        self.__funcionario = funcionario

    def get_codigo_ponto(self):
        return self.__codigo_ponto

    def get_data_ponto(self):
        return self.__data_ponto

    def get_hora_entrada(self):
        return self.__hora_entrada

    def get_hora_saida(self):
        return self.__hora_saida

    def get_funcionario(self):
        return self.__funcionario

    def set_data_ponto(self, data_ponto):
        self.__data_ponto = data_ponto

    def set_hora_entrada(self, hora_entrada):
        self.__hora_entrada = hora_entrada

    def set_hora_saida(self, hora_saida):
        self.__hora_saida = hora_saida

    def set_funcionario(self, funcionario):
        self.__funcionario = funcionario

    def __str__(self):
        try:
            # Tenta converter a string da data para objeto datetime
            if isinstance(self.get_data_ponto(), str):
                data = datetime.strptime(self.get_data_ponto(), '%Y-%m-%d')
            else:
                data = pd.to_datetime(self.get_data_ponto())

            # Formata a data no padrão DD-MM-YYYY
            data_formatada = data.strftime('%d-%m-%Y')
            
            # Formata as horas
            if isinstance(self.get_hora_entrada(), str):
                hora_entrada = datetime.strptime(self.get_hora_entrada(), '%H:%M')
            else:
                hora_entrada = pd.to_datetime(self.get_hora_entrada())
                
            if isinstance(self.get_hora_saida(), str):
                hora_saida = datetime.strptime(self.get_hora_saida(), '%H:%M')
            else:
                hora_saida = pd.to_datetime(self.get_hora_saida())
            
                hora_entrada_formatada = hora_entrada.strftime('%H:%M:%S')
                hora_saida_formatada = hora_saida.strftime('%H:%M:%S')


            return (f"Ponto[ID: {self.get_codigo_ponto()}, Data: {data_formatada}, "
                   f"Entrada: {hora_entrada_formatada}, Saída: {hora_saida_formatada}, "
                   f"Funcionário: {self.get_funcionario().get_nome()}]")
        except Exception as e:
            # Em caso de erro na formatação, retorna os dados no formato original
            return (f"Ponto[ID: {self.get_codigo_ponto()}, Data: {self.get_data_ponto()}, "
                   f"Entrada: {self.get_hora_entrada()}, Saída: {self.get_hora_saida()}, "
                   f"Funcionário: {self.get_funcionario().get_nome()}]")