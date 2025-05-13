# app/Dockerfile

# In order for the git clone to work during building, perform these steps first:
# 1- eval `ssh-agent -s`
# 2- ssh-add ssh-add ~/.ssh/id_rsa (or id_ed25519)
# 3- Then Build with: docker buildx build -t streamlit --ssh default .

FROM python:3.12-slim

WORKDIR /streamlit

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    wget \ 
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install common_library

COPY requirements.txt /streamlit/requirements.txt
RUN pip3 install -r /streamlit/requirements.txt

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "app/streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]