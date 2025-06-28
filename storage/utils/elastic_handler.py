from elasticsearch import Elasticsearch, helpers
import logging

class ElasticsearchHandler:
    def __init__(self, host):
        """Inicializar la conexión con Elasticsearch"""
        self.es = Elasticsearch([host])
        logging.info(f"Conectado a Elasticsearch: {self.es.info()}")
    
    def create_index(self, index_name, mappings=None):
        """Crear un índice con mappings opcionales"""
        if not self.es.indices.exists(index=index_name):
            if mappings:
                self.es.indices.create(index=index_name, body=mappings)
            else:
                self.es.indices.create(index=index_name)
            return True
        return False
    
    def index_document(self, index_name, doc, doc_id=None):
        """Indexar un solo documento"""
        if not self.es.indices.exists(index=index_name):
            self.create_index(index_name)
        
        if doc_id:
            return self.es.index(index=index_name, id=doc_id, body=doc)
        else:
            return self.es.index(index=index_name, body=doc)
    
    def bulk_index(self, index_name, documents):
        """Indexar múltiples documentos en bulk"""
        if not self.es.indices.exists(index=index_name):
            self.create_index(index_name)
        
        actions = [
            {
                "_index": index_name,
                "_source": doc
            }
            for doc in documents
        ]
        
        success, failed = helpers.bulk(self.es, actions, stats_only=True)
        return {"indexed": success, "failed": failed}
    
    def search(self, index_name, query, scroll=None):
        """Realizar una búsqueda en un índice con soporte opcional para scroll"""
        if scroll:
            return self.es.search(index=index_name, body=query, scroll=scroll)
        else:
            return self.es.search(index=index_name, body=query)
    
    def scroll(self, scroll_id, scroll='2m'):
        """Continuar con el scroll para obtener más resultados"""
        return self.es.scroll(scroll_id=scroll_id, scroll=scroll)
    
    def clear_scroll(self, scroll_id):
        """Limpiar un scroll para liberar recursos"""
        try:
            return self.es.clear_scroll(scroll_id=scroll_id)
        except Exception as e:
            logging.warning(f"Error clearing scroll: {e}")
            return {"acknowledged": False, "error": str(e)}
    
    def list_indices(self):
        """Listar todos los índices disponibles"""
        return list(self.es.indices.get_alias("*").keys())
    
    def delete_index(self, index_name):
        """Eliminar un índice completo"""
        if self.es.indices.exists(index=index_name):
            return self.es.indices.delete(index=index_name)
        return {"acknowledged": False, "error": "Index does not exist"}