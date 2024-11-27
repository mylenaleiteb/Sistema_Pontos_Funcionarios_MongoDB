from conexion.oracle_queries import OracleQueries
from utils import config

class SplashScreen:

    def __init__(self):
        self.created_by = "Pierry Jonny, Maria Eduarda, Matheus Castro, Kaylane Simões e Mylena Leite"
        self.professor = "Prof. M.Sc. Howard Roatti"
        self.disciplina = "Banco de Dados"
        self.semestre = "2024/3"

    def get_documents_count(self, collection_name):
        # Retorna o total de registros computado pela query
        df = config.query_count(collection_name=collection_name)
        return df[f"total_{collection_name}"].values[0]
    
    def get_updated_screen(self):
        return f"""
    #################################################################################################################
    #                                          SISTEMA DE CONTROLE DE PONTO                                         #
    #                                                                                                               #
    #  TOTAL DE REGISTROS:                                                                                          #
    #      1 - PONTOS: {str(self.get_documents_count(collection_name="pontos")).ljust(93)}#
    #      2 - FUNCIONARIOS: {str(self.get_documents_count(collection_name="funcionarios")).ljust(87)}#
    #                                                                                                               #
    #  CRIADO POR: {self.created_by.ljust(97)}#
    #                                                                                                               #
    #  PROFESSOR:  {self.professor.ljust(97)}#
    #                                                                                                               #
    #  DISCIPLINA: {self.disciplina.ljust(97)}#
    #              {self.semestre.ljust(97)}#
    #                                                                                                               #
    #################################################################################################################
    """
