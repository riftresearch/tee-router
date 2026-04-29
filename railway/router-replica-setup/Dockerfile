FROM postgres:16

COPY railway/router-replica-setup/setup.sh /setup.sh

RUN chmod 0755 /setup.sh

CMD ["/setup.sh"]
