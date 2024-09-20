#Puxando imagem do Debian bookworm com python 3 instalado
FROM python:3-slim AS develop

#Definindo diretório onde serão realizadas todas ações do arquivo
WORKDIR /usr/src/app

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

#Atualiza o apt-get(Ferramenta para buscar pacotes) e instala xvbf e o firefox, baixa o geckodriver, descomprime e move para a pasta que o selenium utiliza
RUN apt-get update -y && apt-get install xvfb firefox-esr wget -y; \
    wget https://github.com/mozilla/geckodriver/releases/download/v0.35.0/geckodriver-v0.35.0-linux64.tar.gz; \
    tar -x geckodriver -zf geckodriver-v0.35.0-linux64.tar.gz; \
    mv geckodriver /usr/bin/

FROM develop AS prod

#COPY . .
COPY main.py .env ./

RUN mkdir /usr/src/logs && touch /usr/src/logs/success.log /usr/src/logs/error.log

CMD ["/bin/sh", "-c", "python -u ./main.py"]