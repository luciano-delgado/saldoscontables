from hdbcli import dbapi

def conexion_bd(amb):
    if amb == 'QAS':
        conn=dbapi.connect(address="172.31.0.138",port="30115",user="OYP",password="5tAgt7S8k7XvDx",sslValidateCertificate=False)
    elif amb == 'PRD':
        conn = dbapi.connect(address="172.31.0.130",port="30115",user="OYP",password="A112ShhtPLZYVv",sslValidateCertificate=False)  # PRD
    
    return conn


class Conexion:

    def connection_bd(self, amb):       #SELF: para que el método sepa que objeto/instancia lo llamo (si no paso el self sería como que le estoy pasando 2 parametros a la funcion (por las 2 instancias)))
        if amb == 'QAS':
            conn=dbapi.connect(address="172.31.0.138",port="30115",user="OYP",password="5tAgt7S8k7XvDx",sslValidateCertificate=False)
        elif amb == 'PRD':
            conn = dbapi.connect(address="172.31.0.130",port="30115",user="OYP",password="A112ShhtPLZYVv",sslValidateCertificate=False)  # PRD
        
        return conn




