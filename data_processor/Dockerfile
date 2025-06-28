FROM openjdk:8-jdk-slim

# Instalar dependencias necesarias
RUN apt-get update && apt-get install -y wget tar python3 python3-pip curl

# Crear carpetas de datos y resultados
RUN mkdir -p results data scripts

# Descargar e instalar Hadoop 
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz && \
    tar -zxvf hadoop-3.3.4.tar.gz && \
    mv hadoop-3.3.4 /usr/local/hadoop && \
    rm hadoop-3.3.4.tar.gz

# Configurar variables de entorno para Hadoop
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# Descargar e instalar Apache Pig
RUN wget https://downloads.apache.org/pig/pig-0.17.0/pig-0.17.0.tar.gz && \
    tar -zxvf pig-0.17.0.tar.gz && \
    mv pig-0.17.0 /usr/local/pig && \
    rm pig-0.17.0.tar.gz

# Configurar variables de entorno para Pig
ENV PIG_HOME=/usr/local/pig
ENV PATH=$PIG_HOME/bin:$PATH

# Copiar scripts al contenedor
COPY scripts/processor.pig /scripts/processor.pig
COPY scripts/Obetener_data.py /scripts/Obetener_data.py
COPY scripts/requirements.txt /scripts/requirements.txt
COPY scripts/test_pipeline.py /scripts/test_pipeline.py
COPY scripts/run_pipeline.sh /scripts/run_pipeline.sh

# Instalar dependencias de Python
RUN pip3 install -r /scripts/requirements.txt

# Dar permisos de ejecución a los scripts
RUN chmod +x /scripts/Obetener_data.py
RUN chmod +x /scripts/run_pipeline.sh

# Comando por defecto al iniciar
CMD ["/bin/bash", "-c", "python3 /scripts/test_pipeline.py"]