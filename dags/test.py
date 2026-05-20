from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import BaseOperator

# =============================================================================
# APARTADO 5: ¿Qué es un Hook y diferencia con Conexión?
# =============================================================================
"""
Un Hook Es una interfaz de alto nivel que utiliza una Conexión para interactuar directamente con el sistema externo.
Una conexión es un conjunto de credenciales y parámetros de configuración 
(host, usuario, contraseña, puerto) almacenados de forma segura en la base de datos metadatos de Airflow para identificar un sistema externo.
 
El Hook traduce los parámetros de la Conexión en métodos listos para usar (ej. conectar, 
descargar un archivo, ejecutar un query), evitando tener que programar APIs de bajo nivel en las tareas.
"""


# =============================================================================
# APARTADO 4: Creación del Operador Personalizado TimeDiff
# =============================================================================
class TimeDiff(BaseOperator):
    template_fields = ('diff_date',)
    def __init__(self, diff_date: str, **kwargs):
        super().__init__(**kwargs)
        self.diff_date = diff_date

    def execute(self, context):
        try:
            # Parseamos la fecha recibida como entrada (asumiendo formato estándar YYYY-MM-DD)
            target_date = datetime.strptime(self.diff_date, "%Y-%m-%d").date()
            current_date = datetime.now().date()
            
            # Calculamos la diferencia
            difference = current_date - target_date
            
            mensaje = f"La diferencia entre la fecha actual ({current_date}) y la fecha entregada ({target_date}) es de {difference.days} días."
            self.log.info(mensaje)
            print(mensaje)
            return difference.days
        except Exception as e:
            self.log.error(f"Error procesando las fechas: {str(e)}")
            raise e


# =============================================================================
# APARTADO 1: Definición del DAG 'test'
# =============================================================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(1900, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    dag_id='test',
    default_args=default_args,
    description='Tercera Parte: Airflow',
    schedule_interval='0 3 * * *',
    catchup=False
) as dag:

    # =============================================================================
    # APARTADO 2: Tareas 'start' y 'end' usando Dummy/EmptyOperator
    # =============================================================================
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # =============================================================================
    # APARTADO 3: Lista de tareas dinámicas
    # =============================================================================
    N = 6
    lista_tareas_dummy = []

    for n in range(1, N + 1):
        task = EmptyOperator(task_id=f'task_{n}')
        lista_tareas_dummy.append(task)
        
        # Conexión inicial
        start >> task
        
        if n % 2 == 0:
            for i in range(1, n):
                if i % 2 != 0:
                    lista_tareas_dummy[i - 1] >> task

        # Conexión final
        task >> end

    # =============================================================================
    # APARTADO 4 (Continuación)
    # =============================================================================
    # Creamos la nueva tarea usando nuestro operador personalizado pasando una fecha de prueba
    tarea_calculo_tiempo = TimeDiff(
        task_id='tarea_calculo_tiempo',
        diff_date='2026-05-10'
    )

    # Añadimos la tarea personalizada al flujo principal para que corra al final
    end >> tarea_calculo_tiempo