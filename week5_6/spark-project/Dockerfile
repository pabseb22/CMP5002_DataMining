# Capa principal

FROM python:3.11-slim

# instalar el runtime de java puro sin nada mas por que parte de scala
RUN apt-get update && apt-get install -y --no-install-recommends \ 
    openjdk-21-jre openjdk-21-jdk-headless \
    curl

# NEcesitmaos saber la arquitectura dle procesador para ver si es amd. Es importante agregar al path para ejecutar.
ENV JAVA_HOME=/usr/lib/jvm/java-17-opensdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /workspace

COPY requirements.txt /workspace/requirements.txt

RUN pip install --no-cache -r /workspace/requirements.txt

EXPOSE 8888 4040

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--no-browser", "--NotebookApp.token=''", "--NotebookApp.password=''", "--notebook-dir=/workspace"] 


