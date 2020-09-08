FROM alpine
COPY ./output /app
COPY ./output/config /app/config
COPY ./output/logs /app/logs
RUN chmod -R 777 /app
RUN mv /etc/apk/repositories /etc/apk/repositories_bak \
    && echo 'https://mirrors.aliyun.com/alpine/v3.10/main/' >> /etc/apk/repositories \
    && echo 'https://mirrors.aliyun.com/alpine/v3.10/community/' >> /etc/apk/repositories \
    && apk add tzdata \
    && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo Asia/Shanghai > /etc/timezone \
    && apk del tzdata
CMD sh /app/bin/run.sh