# airflow/Dockerfile
FROM apache/airflow:2.10.5

USER root

# 安裝必要工具與 Chrome 相依套件，一次清理 apt
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        bash git zsh vim curl build-essential wget zip make \
        procps gcc python3-dev gnupg ca-certificates \
        fonts-liberation libnss3 libxss1 libatk-bridge2.0-0 \
        libgtk-3-0 libgbm1 libasound2 libx11-xcb1 libxcomposite1 \
        libxcursor1 libxdamage1 libxrandr2 libdrm2 libxfixes3 libxi6 libgl1 unzip && \
    rm -rf /var/lib/apt/lists/*

# 安裝 poetry
RUN curl -sSL https://install.python-poetry.org | python3 - && \
    ln -s /root/.local/bin/poetry /usr/local/bin/poetry

# 複製必要的專案檔案
COPY pyproject.toml poetry.lock /opt/project/

# 安裝 Chrome & Chromedriver（裝完立即刪除 zip）
RUN wget -O chrome.zip https://storage.googleapis.com/chrome-for-testing-public/136.0.7103.49/linux64/chrome-linux64.zip && \
    wget -O chromedriver.zip https://storage.googleapis.com/chrome-for-testing-public/136.0.7103.49/linux64/chromedriver-linux64.zip && \
    unzip chrome.zip && unzip chromedriver.zip && \
    mv chrome-linux64 /opt/chrome && \
    ln -s /opt/chrome/chrome /usr/local/bin/google-chrome && \
    mv chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/google-chrome /usr/local/bin/chromedriver && \
    rm -rf chrome.zip chromedriver.zip chromedriver-linux64

# 設定工作目錄與安裝 Python 套件
WORKDIR /opt/project
RUN poetry config virtualenvs.create false && \
    poetry install --no-root --no-interaction --no-ansi && \
    rm -rf ~/.cache/pip
# ✅ 改成顯式用 python3.12 執行 pip（最穩定）
RUN python3.12 -m pip install --no-cache-dir pendulum

USER airflow
