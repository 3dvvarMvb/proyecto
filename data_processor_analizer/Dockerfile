FROM openjdk:8-jdk-slim

# Instalar dependencias necesarias
RUN apt-get update && apt-get install -y wget tar curl

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
# Copiar piggybank.jar compilado desde carpeta local
COPY libs/piggybank.jar /usr/local/pig/piggybank.jar


# Configurar variables de entorno para Pig
ENV PIG_HOME=/usr/local/pig
ENV PATH=$PIG_HOME/bin:$PATH

# Crear carpetas para resultados y datos
RUN mkdir -p results data

# Copiar scripts
COPY /scripts/processor.pig .
COPY /scripts/export_events.sh .

# Dar permisos de ejecución
RUN chmod +x export_events.sh

# Comando por defecto
CMD ["/bin/bash", "-c", "./export_events.sh && pig -x local processor.pig"]
