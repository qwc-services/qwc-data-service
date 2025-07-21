FROM sourcepole/qwc-uwsgi-base:alpine-v2025.01.24

WORKDIR /srv/qwc_service
ADD pyproject.toml uv.lock ./

# git: Required for pip with git repos
# postgresql-dev g++ python3-dev: Required for psycopg2
# unixodbc-dev: Required for pyodbc (SQL Server support)
RUN \
    apk add --no-cache --update --virtual runtime-deps postgresql-libs unixodbc && \
    apk add --no-cache --update --virtual build-deps git postgresql-dev g++ python3-dev unixodbc-dev && \
    uv sync --frozen && \
    uv cache clean && \
    apk del build-deps

# Install Microsoft ODBC Driver for SQL Server
# Note: Microsoft doesn't provide official Alpine packages, use Ubuntu packages in a separate layer
RUN \
    apk add --no-cache --update curl gnupg && \
    # Create directory for Microsoft repository
    mkdir -p /etc/apt/sources.list.d && \
    # Add Microsoft repository keys and registration
    curl -O https://download.microsoft.com/download/e/4/e/e4e67866-dffd-428c-aac7-8d28ddafb39b/msodbcsql17_17.10.2.1-1_amd64.apk && \
    apk add --allow-untrusted msodbcsql17_17.10.2.1-1_amd64.apk && \
    rm msodbcsql17_17.10.2.1-1_amd64.apk

ADD src /srv/qwc_service/

ENV SERVICE_MOUNTPOINT=/api/v1/data