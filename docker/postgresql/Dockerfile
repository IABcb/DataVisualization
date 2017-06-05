# vim:set ft=dockerfile: #Remember that pgadmin only works with postgres version until 9.3
FROM postgres:9.3

ENV http_proxy="http://www-proxy.ericsson.se:8080" \
	https_proxy="http://www-proxy.ericsson.se:8080" \
	no_proxy="127.0.0.1,localhost,10.65.104.131"

#RUN echo "Acquire::http::Proxy \"http://www-proxy.ericsson.se:8080\";" > /etc/apt/apt.conf

ADD set_up_db.sql /docker-entrypoint-initdb.d/
ADD set_up_subscribers_db.sql /docker-entrypoint-initdb.d/
ADD apache2_start.sh /docker-entrypoint-initdb.d/

# Install Apache 
RUN apt-get update && apt-get install -y apt-utils apache2 
#RUN apt-get install supervisor
# Install php and phppgadmin
RUN apt-get install -y php5 libapache2-mod-php5 php5-mcrypt phppgadmin

RUN sed -i "s/Require local/allow from all/g" /etc/apache2/conf-enabled/phppgadmin.conf
RUN sed -i "s/Require local/allow from all/g" /etc/apache2/conf-available/phppgadmin.conf
RUN sed -i "s/\$conf\['extra_login_security'\] = true;/\$conf\['extra_login_security'\] = false;/g" /etc/phppgadmin/config.inc.php

#ENTRYPOINT ["/usr/bin/supervisord"]
ENTRYPOINT ["/docker-entrypoint.sh"]
#EXPOSE 22
EXPOSE 80
EXPOSE 5432

CMD ["postgres"]


