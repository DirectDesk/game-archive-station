FROM node:22-alpine AS frontend-build
WORKDIR /build/frontend
COPY frontend/package.json ./
COPY frontend/package-lock.json ./
# 使用国内 npm 镜像源：本机网络下 registry.npmjs.org 会被解析到 IPv6 且
# TLS 证书校验失败（ERR_TLS_CERT_ALTNAME_INVALID），导致 npm ci 报出
# 误导性的 "Exit handler never called!"。npmmirror.com 实测可用。
# 注：--registry 不会写入 package-lock.json 的 resolved 字段，仅影响本次安装。
RUN npm ci --registry=https://registry.npmmirror.com
COPY frontend/ ./
RUN npm run build

FROM python:3.11-alpine
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 DATA_DIR=/app/data
RUN apk add --no-cache libstdc++
COPY backend/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY backend/app ./app
COPY backend/tools ./tools
COPY --from=frontend-build /build/frontend/dist ./frontend/dist
RUN mkdir -p /app/data
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
