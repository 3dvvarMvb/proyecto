# Sistema de Análisis de Eventos de Tráfico

## Descripción del Proyecto

El sistema de Análisis de Eventos de Tráfico es una plataforma distribuida que recopila, almacena, procesa y analiza eventos de tráfico en tiempo real. El proyecto utiliza una arquitectura de microservicios containerizados para asegurar alta disponibilidad, escalabilidad y procesamiento eficiente de datos.

La arquitectura incluye:
- Sistema de recolección de datos (scraper)
- Almacenamiento en base de datos NoSQL (Cassandra)
- Cache de consultas frecuentes (Redis)
- API de almacenamiento y consulta (storage-service)
- Procesamiento analítico batch con Apache Pig
- Visualización de estadísticas y tendencias

## Requisitos Previos

1. Tener instalado Docker (versión 20.10.0 o superior)
2. Tener instalado Docker Compose (versión 2.0.0 o superior)
3. Asegurarse de que los siguientes puertos estén disponibles:
   - 9042 (Cassandra)
   - 6379 (Redis)
   - 5000 (API de Storage)

## Configuración Inicial

1. Clonar el repositorio en tu máquina local:
   ```bash
   git clone <url-del-repositorio>
   ```
2. Navegar al directorio raíz del proyecto:
   ```bash
   cd <nombre-del-directorio>
   ```

## Orden de Ejecución de los Contenedores

Los servicios se inician en el siguiente orden para asegurar las dependencias correctas:

1. **Cassandra** - Base de datos NoSQL principal
2. **Redis** - Sistema de caché para consultas frecuentes
3. **Storage** - API de almacenamiento y consulta de datos
4. **Cache-service** - Servicio de gestión de caché
5. **Scraper** - Recolector de datos de tráfico
6. **Pig-service** - Servicio de análisis batch de datos

> **Nota**: Docker Compose manejará automáticamente este orden según las dependencias definidas.

## Instrucciones para Ejecutar el Proyecto

Para construir y levantar todos los servicios con Docker Compose:

```bash
sudo docker-compose up --build
```

Para ejecutar en modo desacoplado (background):

```bash
sudo docker-compose up -d --build
```

## Estructura del Proyecto

- **scraper/**: Recolector de eventos de tráfico
- **storage/**: API RESTful para almacenamiento y consulta
- **cache/**: Servicio de caché para optimización de consultas
- **data_processor_analizer/**: Procesamiento analítico con Apache Pig
  - **scripts/**: Scripts de procesamiento Pig
  - **results/**: Resultados del análisis

## Acceso a los Servicios

- **API de Storage**: http://localhost:5000
  - Endpoints disponibles:
    - GET /events - Lista todos los eventos
    - GET /events/{id} - Obtiene un evento específico
    - POST /events - Registra un nuevo evento

## Monitoreo y Logs

Para ver los logs de un servicio específico:

```bash
sudo docker-compose logs -f [servicio]
```

Donde [servicio] puede ser: cassandra, redis, storage, cache-service, scraper, o pig-service.

## Solución de Problemas

- **Error de conexión a Cassandra**: Verificar que el puerto 9042 esté disponible.
- **Error de memoria en Redis**: Redis está configurado con un límite de 2MB. Puede ajustarse en el docker-compose.yml según sea necesario.
