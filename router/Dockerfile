FROM ubuntu:18.04

RUN apt-get update -y && \
    apt-get install -y nginx && \
    rm -rf /var/lib/apt/lists/*

RUN rm -f /etc/nginx/sites-enabled/default && \
    rm -f /etc/nginx/nginx.conf
ADD nginx.conf /etc/nginx/
ADD router.nginx.conf.in /
ADD proxy.conf /etc/nginx/

RUN ln -sf /dev/stdout /var/log/nginx/access.log
RUN ln -sf /dev/stderr /var/log/nginx/error.log

ADD start-nginx.sh /

CMD ["/start-nginx.sh"]
