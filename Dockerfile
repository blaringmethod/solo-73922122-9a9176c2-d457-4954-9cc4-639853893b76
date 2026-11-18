FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app /app/app

EXPOSE 8000

# 单进程多线程即可：会话状态保存在进程内存中，多 worker 会各自持有
# 独立的会话注册表与名额，不符合语义，因此固定 1 个 worker。
#
# --timeout-graceful-shutdown：收到 SIGTERM 后先让在途 HTTP 请求跑完
# （写请求据此完成提交并返回成功），再进入 lifespan shutdown 关闭写入口、
# 等待在途写结束、最后关闭连接池。必须大于 SHUTDOWN_DRAIN_TIMEOUT_SECONDS。
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", \
     "--workers", "1", "--timeout-graceful-shutdown", "30"]
